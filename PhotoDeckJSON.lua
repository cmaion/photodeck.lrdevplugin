-- JSON encoding and decoding; decoder adapted from rxi/json.lua 0.1.2
-- (dbf4b2dd2eb7c23be2773c89eb059dadd6436f94). Preserves null and object/array
-- types, bounds nesting, and rejects invalid numbers and malformed input.
--
-- Copyright (c) 2020 rxi
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy of
-- this software and associated documentation files (the "Software"), to deal in
-- the Software without restriction, including without limitation the rights to
-- use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
-- of the Software, and to permit persons to whom the Software is furnished to do
-- so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in all
-- copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.

local json = { null = {} }
local arrayType, objectType = {}, {}
local parse
local spaces = { [" "] = true, ["\t"] = true, ["\r"] = true, ["\n"] = true }
local delimiters = { [" "] = true, ["\t"] = true, ["\r"] = true, ["\n"] = true, ["]"] = true, ["}"] = true, [","] = true }
local escapes = { ['"'] = '"', ["\\"] = "\\", ["/"] = "/", b = "\b", f = "\f", n = "\n", r = "\r", t = "\t" }

function json.isArray(value)
  return type(value) == "table" and getmetatable(value) == arrayType
end

function json.isObject(value)
  return type(value) == "table" and getmetatable(value) == objectType
end

function json.array(value)
  return setmetatable(value or {}, arrayType)
end

function json.object(value)
  return setmetatable(value or {}, objectType)
end

local encodeEscapes = { ['"'] = '\\"', ["\\"] = "\\\\", ["\b"] = "\\b", ["\f"] = "\\f", ["\n"] = "\\n", ["\r"] = "\\r", ["\t"] = "\\t" }

local function encodeString(value)
  return '"' .. value:gsub('[%z\1-\31\\"]', function(char)
    return encodeEscapes[char] or string.format("\\u%04x", char:byte())
  end) .. '"'
end

local function encode(value, active, depth)
  assert(depth <= 128, "JSON nesting too deep")
  local kind = type(value)
  if value == json.null or kind == "nil" then
    return "null"
  elseif kind == "string" then
    return encodeString(value)
  elseif kind == "boolean" then
    return tostring(value)
  elseif kind == "number" then
    assert(value == value and value ~= math.huge and value ~= -math.huge, "Invalid JSON number")
    return (string.format("%.17g", value):gsub(",", "."))
  end
  assert(kind == "table", "Unsupported JSON type: " .. kind)
  assert(not active[value], "Circular JSON table")
  active[value] = true
  local result = {}
  local isArray = json.isArray(value) or (not json.isObject(value) and rawget(value, 1) ~= nil)
  if isArray then
    local size, count = #value, 0
    for key in pairs(value) do
      assert(type(key) == "number" and key % 1 == 0 and key >= 1 and key <= size, "Invalid JSON array key")
      count = count + 1
    end
    assert(count == size, "Sparse JSON array")
    for index = 1, size do
      result[index] = encode(value[index], active, depth + 1)
    end
  else
    local keys = {}
    for key in pairs(value) do
      assert(type(key) == "string", "JSON object keys must be strings")
      keys[#keys + 1] = key
    end
    table.sort(keys)
    for _, key in ipairs(keys) do
      result[#result + 1] = encodeString(key) .. ":" .. encode(value[key], active, depth + 1)
    end
  end
  active[value] = nil
  local body = table.concat(result, ",")
  return isArray and ("[" .. body .. "]") or ("{" .. body .. "}")
end

-- Plain empty tables are objects; use json.array() for an empty JSON array.
function json.encode(value)
  return encode(value, {}, 0)
end

local function decodeError(index, message)
  error(string.format("Invalid JSON at byte %d: %s", index, message), 0)
end

local function skipSpace(str, index)
  while spaces[str:sub(index, index)] do
    index = index + 1
  end
  return index
end

local function utf8(n)
  local floor = math.floor
  if n <= 0x7f then
    return string.char(n)
  elseif n <= 0x7ff then
    return string.char(floor(n / 64) + 192, n % 64 + 128)
  elseif n <= 0xffff then
    return string.char(floor(n / 4096) + 224, floor(n % 4096 / 64) + 128, n % 64 + 128)
  end
  return string.char(floor(n / 262144) + 240, floor(n % 262144 / 4096) + 128, floor(n % 4096 / 64) + 128, n % 64 + 128)
end

local function parseString(str, index)
  local result, start = {}, index + 1
  index = start
  while index <= #str do
    local byte = str:byte(index)
    if byte < 32 then
      decodeError(index, "control character in string")
    elseif byte == 34 then
      result[#result + 1] = str:sub(start, index - 1)
      return table.concat(result), index + 1
    elseif byte == 92 then
      result[#result + 1] = str:sub(start, index - 1)
      index = index + 1
      local char = str:sub(index, index)
      if char == "u" then
        local hex = str:sub(index + 1, index + 4)
        if not hex:match("^%x%x%x%x$") then
          decodeError(index, "invalid Unicode escape")
        end
        local code = tonumber(hex, 16)
        index = index + 4
        if code >= 0xd800 and code <= 0xdbff then
          local lowHex = str:sub(index + 3, index + 6)
          local low = lowHex:match("^%x%x%x%x$") and tonumber(lowHex, 16)
          if str:sub(index + 1, index + 2) ~= "\\u" or not low or low < 0xdc00 or low > 0xdfff then
            decodeError(index, "invalid surrogate pair")
          end
          code = (code - 0xd800) * 0x400 + low - 0xdc00 + 0x10000
          index = index + 6
        elseif code >= 0xdc00 and code <= 0xdfff then
          decodeError(index, "unpaired surrogate")
        end
        result[#result + 1] = utf8(code)
      else
        if not escapes[char] then
          decodeError(index, "invalid string escape")
        end
        result[#result + 1] = escapes[char]
      end
      start = index + 1
    end
    index = index + 1
  end
  decodeError(index, "unterminated string")
end

local function parseNumber(str, index)
  local finish = index
  while finish <= #str and not delimiters[str:sub(finish, finish)] do
    finish = finish + 1
  end
  local token = str:sub(index, finish - 1)
  local integer, rest = token:match("^(-?%d+)(.*)$")
  if not integer or integer:match("^-?0%d") then
    decodeError(index, "invalid number")
  end
  if rest:sub(1, 1) == "." then
    rest = rest:match("^%.%d+(.*)$")
  end
  if not rest or (rest ~= "" and not rest:match("^[eE][+-]?%d+$")) then
    decodeError(index, "invalid number")
  end
  local value = tonumber(token)
  if not value or value == math.huge or value == -math.huge then
    decodeError(index, "invalid number")
  end
  return value, finish
end

local function parseCollection(str, index, depth, object)
  local result = setmetatable({}, object and objectType or arrayType)
  local closing = object and "}" or "]"
  index = skipSpace(str, index + 1)
  if str:sub(index, index) == closing then
    return result, index + 1
  end
  local count = 0
  while true do
    local key, value
    if object then
      if str:sub(index, index) ~= '"' then
        decodeError(index, "expected object key")
      end
      key, index = parseString(str, index)
      index = skipSpace(str, index)
      if str:sub(index, index) ~= ":" then
        decodeError(index, "expected colon")
      end
      index = skipSpace(str, index + 1)
    else
      count = count + 1
      key = count
    end
    value, index = parse(str, index, depth + 1)
    result[key] = value
    index = skipSpace(str, index)
    local char = str:sub(index, index)
    if char == closing then
      return result, index + 1
    end
    if char ~= "," then
      decodeError(index, "expected comma or closing delimiter")
    end
    index = skipSpace(str, index + 1)
  end
end

parse = function(str, index, depth)
  if depth > 128 then
    decodeError(index, "nesting too deep")
  end
  local char = str:sub(index, index)
  if char == '"' then
    return parseString(str, index)
  elseif char == "{" or char == "[" then
    return parseCollection(str, index, depth, char == "{")
  elseif char == "-" or char:match("%d") then
    return parseNumber(str, index)
  end
  for token, value in pairs({ ["true"] = true, ["false"] = false, ["null"] = json.null }) do
    local finish = index + #token
    if str:sub(index, finish - 1) == token and (finish > #str or delimiters[str:sub(finish, finish)]) then
      return value, finish
    end
  end
  decodeError(index, "unexpected token")
end

function json.decode(str)
  assert(type(str) == "string", "JSON input must be a string")
  local result, index = parse(str, skipSpace(str, 1), 0)
  if skipSpace(str, index) <= #str then
    decodeError(index, "trailing data")
  end
  return result
end

return json
