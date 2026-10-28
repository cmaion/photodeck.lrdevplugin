-- Run from the plugin directory: lua5.1 tests/run.lua
-- No Lightroom installation, network access or real credentials are used.
package.path = "./?.lua;" .. package.path

local tests, env = {}, nil
local realIO = io
local apiOrigin = "https://api.example.test"
local currentSection

local function section(name)
  currentSection = name
end

local function test(name, run)
  assert(currentSection, "Tests must belong to a section")
  tests[#tests + 1] = { name = name, run = run, section = currentSection }
end

-- Assertions and HTTP fixtures

local function display(value)
  if type(value) ~= "string" then
    return tostring(value)
  end
  if #value > 120 then
    return string.format("%q... (%d bytes)", value:sub(1, 120), #value)
  end
  return string.format("%q", value)
end

local function equal(actual, expected, context)
  if actual ~= expected then
    local prefix = context and (context .. ": ") or ""
    error(prefix .. string.format("expected %s, got %s", display(expected), display(actual)), 2)
  end
end
local function near(actual, expected)
  if not (math.abs(actual - expected) < 0.00000001) then
    error(string.format("expected %.12f, got %.12f", expected, actual), 2)
  end
end
local function contains(actual, expected)
  if not actual or not actual:find(expected, 1, true) then
    error("missing: " .. display(expected) .. " in " .. display(actual), 2)
  end
end
local function absent(actual, unexpected)
  if actual:find(unexpected, 1, true) then
    error("unexpected: " .. display(unexpected) .. " in " .. display(actual), 2)
  end
end
local function response(status, body, headers, contentType)
  return { status = status, body = body, headers = headers or {}, contentType = contentType or "application/json; charset=utf-8" }
end
local function rateHeaders(limit, remaining, reset)
  local headers = {}
  if limit ~= nil then
    headers[#headers + 1] = { field = "RateLimit-Limit", value = tostring(limit) }
  end
  if remaining ~= nil then
    headers[#headers + 1] = { field = "RateLimit-Remaining", value = tostring(remaining) }
  end
  if reset ~= nil then
    headers[#headers + 1] = { field = "RateLimit-Reset", value = tostring(reset) }
  end
  return headers
end
local function enqueue(...)
  for index = 1, select("#", ...) do
    env.responses[#env.responses + 1] = select(index, ...)
  end
end
local function header(request, name)
  for _, item in ipairs(request.headers) do
    if item.field:lower() == name:lower() then
      return item.value
    end
  end
end
local function payload(request)
  equal(header(request, "Content-Type"), "application/json")
  local json = require("PhotoDeckJSON")
  local value = json.decode(request.body)
  assert(json.isObject(value), "API request payload must be a JSON object")
  return value
end
local function count(tableValue)
  local result = 0
  for _ in pairs(tableValue) do
    result = result + 1
  end
  return result
end

-- Lightroom mocks; every test starts with isolated modules, HTTP and clock state.

local function reset()
  _G.io = realIO
  env = { responses = {}, calls = {}, signatures = {}, logs = {}, now = 0, otpCodes = {} }
  local function send(method, url, body, headers, totalSize, transport)
    local chunks = {}
    if type(body) == "function" then
      while true do
        -- Native byte-getter callbacks are not ordinary asynchronous tasks.
        env.inBodyGetter = true
        local ok, chunk = pcall(body)
        env.inBodyGetter = false
        if not ok then
          error(chunk, 0)
        end
        if not chunk then
          break
        end
        chunks[#chunks + 1] = chunk
      end
      body = table.concat(chunks)
      if type(totalSize) ~= "number" or totalSize ~= #body then
        env.unexpected = "Stream size does not match the SDK totalSize argument"
      end
    end
    local request = { method = method, url = url, body = body or "", headers = headers, time = env.now, chunks = chunks, transport = transport or "post" }
    env.calls[#env.calls + 1] = request
    local item = table.remove(env.responses, 1)
    if not item then
      env.unexpected = "Unexpected HTTP call: " .. method .. " " .. url
      return nil, { status = 0 }
    end
    if item.inspect then
      item.inspect(request)
    end
    env.now = env.now + (item.duration or 0)
    local resultHeaders = { status = item.status }
    resultHeaders.error = item.error
    resultHeaders[#resultHeaders + 1] = { field = "Content-Type", value = item.contentType }
    for _, value in ipairs(item.headers) do
      resultHeaders[#resultHeaders + 1] = value
    end
    return item.body, resultHeaders
  end
  local factory = setmetatable({}, {
    __index = function(_, key)
      if key == "dialog_spacing" then
        return function()
          return 1
        end
      end
      return function(_, value)
        return value
      end
    end,
  })
  env.modules = {
    LrDate = {
      currentTime = function()
        return env.now
      end,
      timestampToComponents = function(time)
        local date = os.date("!*t", time + 978307200)
        return date.year, date.month, date.day, date.hour, date.min, date.sec, date.wday
      end,
      timeFromComponents = function(year, month, day, hour, minute, second)
        local time = os.time({ year = year, month = month, day = day, hour = hour, min = minute, sec = second, isdst = false })
        local utc = os.date("!*t", time)
        utc.isdst = false
        return time + os.difftime(time, os.time(utc)) - 978307200
      end,
    },
    LrDigest = { SHA1 = {
      digest = function(value)
        env.signatures[#env.signatures + 1] = value
        return "ABCDEF"
      end,
    } },
    LrHttp = {
      get = function(url, headers)
        return send("GET", url, nil, headers)
      end,
      post = function(url, body, headers, method, timeout, totalSize)
        return send(method or "POST", url, body, headers, totalSize)
      end,
      postMultipart = function(url, params, headers)
        env.nativeMultipart = { params = params, headers = headers }
        local chunks, nativeHeaders = {}, {}
        for _, part in ipairs(params) do
          chunks[#chunks + 1] = '--sdk-boundary\r\nContent-Disposition: form-data; name="' .. part.name .. '"'
          if part.filePath then
            chunks[#chunks + 1] = '; filename="' .. part.fileName .. '"\r\nContent-Type: ' .. part.contentType
            local file = assert(io.open(part.filePath, "rb"))
            chunks[#chunks + 1] = "\r\n\r\n" .. file:read("*a") .. "\r\n"
            file:close()
          else
            chunks[#chunks + 1] = "\r\n\r\n" .. part.value .. "\r\n"
          end
        end
        chunks[#chunks + 1] = "--sdk-boundary--\r\n"
        for _, item in ipairs(headers or {}) do
          nativeHeaders[#nativeHeaders + 1] = item
        end
        nativeHeaders[#nativeHeaders + 1] = { field = "Content-Type", value = "multipart/form-data; boundary=sdk-boundary" }
        return send("POST", url, table.concat(chunks), nativeHeaders, nil, "postMultipart")
      end,
      openUrlInBrowser = function(url)
        env.browserURL = url
      end,
    },
    LrFileUtils = {
      fileAttributes = function(path)
        local file = io.open(path, "rb")
        if not file then
          return nil
        end
        local size = file:seek("end")
        file:close()
        return { fileSize = size }
      end,
      delete = function() end,
    },
    LrPathUtils = {
      leafName = function(path)
        return path:match("[^/]+$")
      end,
    },
    LrStringUtils = {
      encodeBase64 = function(value)
        return "base64(" .. value .. ")"
      end,
    },
    LrTasks = {
      sleep = function(seconds)
        env.now = env.now + seconds
      end,
      pcall = pcall,
      startAsyncTask = function(action)
        action()
      end,
    },
    LrView = {
      osFactory = function()
        return factory
      end,
      bind = function(value)
        return value
      end,
      share = function(value)
        return value
      end,
    },
    LrDialogs = {
      presentModalDialog = function(dialog)
        env.otpPrompts = (env.otpPrompts or 0) + 1
        local code = table.remove(env.otpCodes, 1)
        dialog.contents.bind_to_object.otp = code
        return code and "ok" or "cancel"
      end,
    },
    LrLogger = function()
      return {
        enable = function() end,
        quick = function(_, ...)
          local result = {}
          for index = 1, select("#", ...) do
            result[index] = function(message)
              env.logs[#env.logs + 1] = message
            end
          end
          return unpack(result)
        end,
      }
    end,
    LrBinding = {
      keyIsNotNil = function(value)
        return value
      end,
    },
    LrProgressScope = {},
    LrApplication = {
      activeCatalog = function()
        return env.catalog
      end,
    },
    LrErrors = {
      throwUserError = function(message)
        error(message, 0)
      end,
    },
  }
  _G.import = function(name)
    return assert(env.modules[name], "Unmocked module " .. name)
  end
  _G.LOC = function(value, ...)
    local result = value:match("=(.*)") or value
    local args = { ... }
    return (result:gsub("%^(%d+)", function(index)
      return tostring(args[tonumber(index)])
    end))
  end
  _G._PLUGIN = {}
  for _, name in ipairs({ "PhotoDeckAPI", "PhotoDeckHttp", "PhotoDeckJSON", "PhotoDeckDialogs", "PhotoDeckPublishServiceProvider" }) do
    package.loaded[name] = nil
  end
  -- API-level and direct HTTP-client tests use the same fake origin.
  local transport = require("PhotoDeckHttp")
  local newClient = transport.new
  transport.new = function(state)
    return newClient(state, apiOrigin)
  end
  env.api = require("PhotoDeckAPI")
  transport.new = newClient
  env.api.connect("test-key", "test-secret", "user@example.com", "test-password")
end

local function httpClient()
  local state = { key = "test-key", secret = "test-secret", username = "user@example.com", password = "test-password", loggedin = false }
  return require("PhotoDeckHttp").new(state, apiOrigin), state
end

local function cooperativeTasks()
  -- Lua 5.1 pcall cannot yield; emulate LrTasks.pcall for interleaved requests.
  local function pack(...)
    return { n = select("#", ...), ... }
  end
  env.modules.LrTasks.pcall = function(action)
    local thread = coroutine.create(action)
    local result = pack(coroutine.resume(thread))
    while result[1] and coroutine.status(thread) ~= "dead" do
      local values = pack(coroutine.yield(unpack(result, 2, result.n)))
      result = pack(coroutine.resume(thread, unpack(values, 1, values.n)))
    end
    return unpack(result, 1, result.n)
  end
  env.modules.LrTasks.sleep = function()
    coroutine.yield("waiting")
  end
end

local fixture = "tests/fixtures/upload.bin"
local function instructions(options)
  options = options or {}
  return '{"uuid":"m","file_name":"returned.jpg","mime_type":"image/jpeg","upload_location":"'
    .. (options.location or "primary")
    .. '","upload_url":"https://storage.example.test/file?secret=signed-value","upload_method":"'
    .. (options.method or "PUT")
    .. '","upload_file_param":'
    .. (options.multipart and '"file"' or "null")
    .. ',"upload_params":'
    .. (options.params or '{"policy":"storage-policy","key":"object-key"}')
    .. ',"upload_expires_in":'
    .. (options.expiry or 3600)
    .. "}"
end
local function uploadAttributes()
  return { contentPath = fixture, artistId = "library", publishToGallery = "gallery", watermarkId = "none", mimeType = "image/tiff" }
end

section("JSON encoding and decoding")

test("JSON preserves nulls, array/object types, booleans and Unicode", function()
  local json = require("PhotoDeckJSON")
  local value = json.decode('{"empty":[],"object":{},"array":[null,false,0],"name":"Caf\\u00e9 \\ud83d\\ude00","literal":"[====[ harmless ]====]"}')
  assert(json.isObject(value) and json.isArray(value.empty) and json.isObject(value.object))
  equal(#value.array, 3)
  equal(value.array[1], json.null)
  equal(value.array[2], false)
  equal(value.array[3], 0)
  equal(value.name, "Café 😀")
  equal(json.decode("-1.25e+2"), -125)
end)

test("JSON rejects malformed input without evaluating code", function()
  local json = require("PhotoDeckJSON")
  for _, value in ipairs({
    "",
    "return {}",
    "[1,]",
    '{"a":1,}',
    "01",
    "0x10",
    "1.",
    "1e",
    "1e999",
    '"\\ud800"',
    '"\\ud800\\u0001"',
    '"\\udc00"',
    '"\\x"',
    '"\n"',
    "{} trailing",
    string.rep("[", 130) .. string.rep("]", 130),
  }) do
    assert(not pcall(json.decode, value), "accepted " .. value)
  end
end)

test("JSON encoding preserves nested objects, arrays, native types and UTF8", function()
  local json = require("PhotoDeckJSON")
  local shared = { value = 7 }
  local source = {
    media = { title = 'Café 😀 "quote" \\ \b\f\n\r\t\000\031', enabled = true, hidden = false, count = 0, duration = 1.25 },
    array = { "one", false, json.null, shared },
    emptyArray = json.array(),
    emptyObject = json.object(),
    other = shared,
  }
  local encoded = json.encode(source)
  local decoded = json.decode(encoded)
  equal(decoded.media.title, source.media.title)
  equal(decoded.media.enabled, true)
  equal(decoded.media.hidden, false)
  equal(decoded.media.count, 0)
  equal(decoded.media.duration, 1.25)
  equal(decoded.array[2], false)
  equal(decoded.array[3], json.null)
  equal(decoded.array[4].value, 7)
  equal(decoded.other.value, 7)
  assert(json.isArray(decoded.emptyArray))
  assert(json.isObject(decoded.emptyObject))
  equal(json.encode({}), "{}")
  equal(json.encode(nil), "null")
  equal(json.encode(json.decode('{"a":[],"b":{},"c":[null]}')), '{"a":[],"b":{},"c":[null]}')
  contains(encoded, '\\"quote\\"')
  contains(encoded, "\\u0000")
  for byte = 0, 31 do
    equal(json.decode(json.encode(string.char(byte))), string.char(byte))
  end
end)

test("JSON encoding rejects non-finite numbers, unsupported values and invalid tables", function()
  local json = require("PhotoDeckJSON")
  local circular = {}
  circular.self = circular
  local deep = {}
  for _ = 1, 130 do
    deep = { child = deep }
  end
  for _, case in ipairs({
    { name = "NaN", value = 0 / 0 },
    { name = "infinity", value = math.huge },
    { name = "negative infinity", value = -math.huge },
    { name = "function", value = function() end },
    { name = "circular table", value = circular },
    { name = "deep nesting", value = deep },
    { name = "sparse array", value = { [1] = true, [3] = false } },
    { name = "mixed keys", value = { "one", other = true } },
    { name = "numeric object key", value = json.object({ [1] = "one" }) },
    { name = "non-numeric array key", value = json.array({ other = true }) },
  }) do
    assert(not pcall(json.encode, case.value), "Accepted " .. case.name)
  end
end)

section("Request signing and encoding")

test("signing uses exact encoded path/query, SHA1, English RFC2822 and trailing newline", function()
  local client = httpClient()
  enqueue(response(200, "{}"), response(200, "{}"))
  assert(client.request("GET", "/medias.json?filter%5Bsearch%5D=%2b+c%2Fz&per_page=2"))
  equal(env.signatures[1], "GET\n/medias.json\nfilter%5Bsearch%5D=%2b+c%2Fz&per_page=2\ntest-secret\nMon, 01 Jan 2001 00:00:00 GMT\n")
  equal(header(env.calls[1], "X-PhotoDeck-Authorization"), "test-key:abcdef")
  assert(client.request("PUT", "/medias/m.json?include=gallery_uuids", { media = { title = "Café + &" } }))
  contains(env.signatures[2], "PUT\n/medias/m.json\ninclude=gallery_uuids\n")
  equal(env.calls[2].body, '{"media":{"title":"Café + &"}}')
  equal(payload(env.calls[2]).media.title, "Café + &")
end)

test("GET encoding handles bracket names, UTF8, booleans and repeated arrays", function()
  local client = httpClient()
  enqueue(response(200, "[]"))
  assert(client.request("GET", "/medias.json", { ["filter[search]"] = "+Café", ["filter[published]"] = false, ["filter[medias_uuid][]"] = { "one", "two" } }))
  contains(env.calls[1].url, "filter%5Bmedias_uuid%5D%5B%5D=one&filter%5Bmedias_uuid%5D%5B%5D=two")
  contains(env.calls[1].url, "filter%5Bpublished%5D=0")
  contains(env.calls[1].url, "filter%5Bsearch%5D=%2BCaf%C3%A9")
  equal(env.calls[1].body, "")
  equal(header(env.calls[1], "Content-Type"), nil)
end)

test("POST, PUT, PATCH and DELETE send nested JSON with native types", function()
  local client = httpClient()
  local json = require("PhotoDeckJSON")
  for _, method in ipairs({ "POST", "PUT", "PATCH", "DELETE" }) do
    enqueue(response(200, "{}"))
    assert(client.request(method, "/medias.json?include=gallery_uuids", {
      media = { title = "", enabled = true, hidden = false, file_size = 1234, rating = 0, omitted = nil, gallery_uuids = json.array() },
      select = "one,two",
    }))
    local call = env.calls[#env.calls]
    local body = payload(call)
    equal(call.method, method)
    equal(call.url, apiOrigin .. "/medias.json?include=gallery_uuids")
    equal(body.select, "one,two")
    equal(body.media.title, "")
    equal(body.media.enabled, true)
    equal(body.media.hidden, false)
    equal(body.media.file_size, 1234)
    equal(body.media.rating, 0)
    equal(body.media.omitted, nil)
    assert(json.isArray(body.media.gallery_uuids))
    equal(#body.media.gallery_uuids, 0)
    absent(call.body, "omitted")
    absent(call.body, "media[")
  end
end)

section("Authentication and sessions")

test("login retains all cookies without decoding their values", function()
  local client, state = httpClient()
  enqueue(
    response(200, '{"email":"user@example.com"}', {
      { field = "Set-Cookie", value = "_ficelle_session=a%2Bb; Path=/; Secure; HttpOnly" },
      { field = "set-cookie", value = "routing=shard-2; Path=/; Secure" },
    }),
    response(200, "[]")
  )
  assert(client.request("GET", "/whoami.json"))
  assert(state.loggedin)
  assert(client.request("GET", "/artists.json"))
  equal(header(env.calls[1], "Authorization"), "Basic base64(user@example.com:test-password)")
  equal(header(env.calls[2], "Authorization"), nil)
  contains(header(env.calls[2], "Cookie"), "_ficelle_session=a%2Bb")
  contains(header(env.calls[2], "Cookie"), "routing=shard-2")
  equal(#env.signatures, 2)
end)

test("ping neither sends user credentials nor establishes a user session", function()
  enqueue(response(200, '{"notice":"OK"}'))
  equal(env.api.ping(), "OK")
  equal(env.calls[1].method, "GET")
  assert(env.calls[1].url:match("/ping%.json$"))
  contains(env.signatures[1], "GET\n/ping.json\n\n")
  equal(env.api.loggedin, false)
  equal(header(env.calls[1], "Authorization"), nil)
end)

test("OTP hint prompts once, appends code once and uses session afterwards", function()
  env.otpCodes = { "123456" }
  enqueue(response(401, '{"error":"Code required","auth_method":"basic:password+otp"}'), response(200, '{"email":"user@example.com"}'), response(200, "[]"))
  assert(env.api.whoami())
  equal(env.otpPrompts, 1)
  equal(header(env.calls[2], "Authorization"), "Basic base64(user@example.com:test-password+123456)")
  assert(env.api.galleries("site"))
  equal(header(env.calls[3], "Authorization"), nil)
  equal(env.api.otp, nil)
end)

test("invalid password and canceled OTP do not loop", function()
  enqueue(response(401, '{"error":"Incorrect password"}'))
  local value, message = env.api.whoami()
  equal(value, nil)
  equal(message, "Incorrect password")
  equal(env.otpPrompts, nil)
  enqueue(response(401, '{"error":"Code required","auth_method":"basic:password+otp"}'))
  value, message = env.api.whoami()
  equal(value, nil)
  equal(env.otpPrompts, 1)
  equal(#env.calls, 2)
end)

test("expired sessions reauthenticate and retry the original request", function()
  local client, state = httpClient()
  state.loggedin = true
  enqueue(response(401, '{"error":"Session expired"}'), response(200, "[]"))
  assert(client.request("GET", "/artists.json"))
  equal(#env.calls, 2)
  equal(header(env.calls[1], "Authorization"), nil)
  equal(header(env.calls[2], "Authorization"), "Basic base64(user@example.com:test-password)")
end)

test("204 deletion/logout need no JSON and clear the session", function()
  enqueue(response(204, nil, {}, "text/plain"), response(204, "", {}, "text/plain"))
  local _, message = env.api.deletePhoto("m")
  equal(message, nil)
  _, message = env.api.logout()
  equal(message, nil)
  equal(env.api.loggedin, false)
  equal(#env.calls, 2)
end)

test("cookies respect path boundaries, expiration and explicit reset", function()
  local client = httpClient()
  enqueue(
    response(200, "{}", {
      { field = "Set-Cookie", value = "scoped=yes; Path=/medias" },
      { field = "Set-Cookie", value = "deleted=value; Path=/; Max-Age=0" },
      { field = "Set-Cookie", value = "foreign=value; Domain=evil.example; Path=/" },
    }),
    response(200, "{}"),
    response(200, "{}"),
    response(200, "{}")
  )
  assert(client.request("GET", "/whoami.json"))
  assert(client.request("GET", "/medias/m.json"))
  equal(header(env.calls[2], "Cookie"), "scoped=yes")
  assert(client.request("GET", "/medias_other.json"))
  equal(header(env.calls[3], "Cookie"), "")
  client.reset()
  assert(client.request("GET", "/medias/m.json"))
  equal(header(env.calls[4], "Cookie"), "")
end)

test("account switching clears cached resources and credentials", function()
  enqueue(response(200, '[{"uuid":"old-style","name":"Old"}]'), response(204, ""), response(200, '[{"uuid":"new-style","name":"New"}]'))
  assert(env.api.galleryDisplayStyles("site"))
  env.api.connect("test-key", "test-secret", "other@example.com", "other-password")
  equal(assert(env.api.galleryDisplayStyles("site"))["old-style"], nil)
  equal(header(env.calls[3], "Cookie"), "")
  equal(header(env.calls[3], "Authorization"), "Basic base64(other@example.com:other-password)")
  equal(#env.calls, 3)
end)

section("HTTP responses, redirects and retries")

test("307 preserves the JSON body and PUT method, retains cookies and signs destination", function()
  local client = httpClient()
  enqueue(
    response(307, "", {
      { field = "Location", value = "/medias/m.json?routing=second%2Bnode" },
      { field = "Set-Cookie", value = "routing=node-2; Path=/" },
    }),
    response(200, '{"uuid":"m"}')
  )
  local params = { media = { title = "A title", publish_to_gallery_uuids = { "one", "two" } } }
  assert(client.request("PUT", "/medias/m.json", params))
  equal(env.calls[2].method, "PUT")
  equal(env.calls[2].body, env.calls[1].body)
  local body = payload(env.calls[2])
  equal(body.media.title, "A title")
  equal(#body.media.publish_to_gallery_uuids, 2)
  equal(body.media.publish_to_gallery_uuids[2], "two")
  equal(header(env.calls[2], "Cookie"), "routing=node-2")
  contains(env.signatures[2], "PUT\n/medias/m.json\nrouting=second%2Bnode\n")
  absent(env.calls[2].body, "_method")
end)

test("redirects and pagination cannot forward credentials to another origin", function()
  enqueue(response(307, "", { { field = "Location", value = "https://evil.example/medias.json" } }))
  local value, message = env.api.galleries("site")
  equal(value, nil)
  assert(message)
  equal(#env.calls, 1)
  enqueue(response(200, "[]", { { field = "Link", value = '<https://evil.example/artists.json>; rel="next"' } }))
  value, message = env.api.artists()
  equal(value, nil)
  assert(message)
  equal(#env.calls, 2)
end)

test("redirect retries are bounded", function()
  for _ = 1, 4 do
    enqueue(response(307, "", { { field = "Location", value = "/artists.json" } }))
  end
  local client = httpClient()
  local result, message = client.request("GET", "/artists.json")
  equal(result, nil)
  assert(message)
  equal(#env.calls, 4)
end)

test("transient GET retries are bounded and ambiguous mutations are not replayed", function()
  local client = httpClient()
  for _ = 1, 4 do
    enqueue(response(503, "upstream unavailable", {}, "text/html"))
  end
  local value, message = client.request("GET", "/artists.json")
  equal(value, nil)
  contains(message, "503")
  equal(#env.calls, 4)
  enqueue(response(0, nil))
  value, message = client.request("POST", "/medias.json", {})
  equal(value, nil)
  assert(message)
  equal(#env.calls, 5)
end)

test("malformed successful bodies and non-JSON errors are reported safely", function()
  enqueue(response(200, "not JSON"), response(403, "proxy error", {}, "text/html"))
  local value, message = env.api.getPhoto("m")
  equal(value, nil)
  assert(message)
  value, message = env.api.getPhoto("m")
  equal(value, nil)
  contains(message, "403")
  equal(#env.calls, 2)
end)

test("HTML 403 rejects JSON mutations without retrying or clearing the session", function()
  local client, state = httpClient()
  state.loggedin = true
  enqueue(response(403, "<!DOCTYPE html><h1>Access denied</h1>", {}, "text/html; charset=utf-8"))
  local result, message = client.request("PUT", "/medias/m.json", { media = { title = "Title" } })
  equal(result, nil)
  equal(message, "HTTP error 403")
  equal(#env.calls, 1)
  equal(state.loggedin, true)
  equal(payload(env.calls[1]).media.title, "Title")
end)

section("Rate limiting")

test("429 honors Retry-After even when longer than two minutes", function()
  enqueue(response(429, '{"error":"Wait"}', { { field = "Retry-After", value = "180" } }), response(200, "[]"))
  assert(env.api.galleries("site"))
  assert(env.calls[2].time - env.calls[1].time >= 180)
end)

test("Retry-After supports HTTP dates", function()
  enqueue(response(429, '{"error":"Wait"}', { { field = "Retry-After", value = "Mon, 01 Jan 2001 00:05:00 GMT" } }), response(200, "[]"))
  assert(env.api.galleries("site"))
  equal(env.calls[2].time, 300)
end)

for _, case in ipairs({
  { name = "seconds", value = "180", delay = 180 },
  { name = "HTTP dates", value = "Mon, 01 Jan 2001 00:05:00 GMT", delay = 300 },
}) do
  test("HTML 429 honors Retry-After " .. case.name .. " and preserves JSON mutation bodies", function()
    local client = httpClient()
    enqueue(
      response(429, "<!DOCTYPE html><h1>Too many requests</h1>", { { field = "Retry-After", value = case.value } }, "text/html"),
      response(200, '{"uuid":"m"}')
    )
    assert(client.request("PUT", "/medias/m.json", { media = { title = "Café", publish_to_gallery_uuids = { "gallery" } } }))
    equal(#env.calls, 2)
    equal(env.calls[2].time, case.delay)
    equal(env.calls[2].body, env.calls[1].body)
    equal(payload(env.calls[2]).media.title, "Café")
  end)
end

test("default API throttle is one request per second; headers can permit faster", function()
  local client = httpClient()
  enqueue(
    response(200, "[]"),
    response(200, "[]", {
      { field = "RateLimit-Limit", value = "100" },
      { field = "RateLimit-Remaining", value = "99" },
      { field = "RateLimit-Reset", value = "1" },
    }),
    response(200, "[]")
  )
  assert(client.request("GET", "/artists.json"))
  assert(client.request("GET", "/artists.json"))
  assert(client.request("GET", "/artists.json"))
  assert(env.calls[2].time - env.calls[1].time >= 1)
  assert(env.calls[3].time - env.calls[2].time < 1)
end)

test("traffic smoothing scales with quota utilization and the full policy window", function()
  local client = httpClient()
  for _, remaining in ipairs({ 54, 30, 6 }) do
    enqueue(response(200, "[]", rateHeaders("60, 5;w=1, 60;w=60", remaining, 1)))
  end
  enqueue(response(200, "[]"))
  for _ = 1, 4 do
    assert(client.request("GET", "/artists.json"))
  end
  near(env.calls[2].time - env.calls[1].time, 60 * 0.1 ^ 10)
  near(env.calls[3].time - env.calls[2].time, 60 * 0.5 ^ 10)
  near(env.calls[4].time - env.calls[3].time, 60 * 0.9 ^ 10)
  assert(env.calls[4].time - env.calls[3].time > 20)
end)

test("multi-policy headers use the longest window whose quota fits the capacity", function()
  local client = httpClient()
  enqueue(
    response(200, "[]", rateHeaders("60,5; w=1,60;w=60,120;w=3600", 6, 1)),
    response(200, "[]", rateHeaders("100, 100;w=300, 999;w=1200", 20, 1)),
    response(200, "[]")
  )
  for _ = 1, 3 do
    assert(client.request("GET", "/artists.json"))
  end
  near(env.calls[2].time - env.calls[1].time, 60 * 0.9 ^ 10)
  near(env.calls[3].time - env.calls[2].time, 300 * 0.8 ^ 10)
end)

test("cached quota information supports partial and missing rate-limit headers", function()
  local client = httpClient()
  enqueue(response(200, "[]", rateHeaders("60,60;w=60", 6, 15)), response(200, "[]", rateHeaders(nil, 6, nil)), response(200, "[]"), response(200, "[]"))
  for _ = 1, 4 do
    assert(client.request("GET", "/artists.json"))
  end
  for index = 2, 4 do
    near(env.calls[index].time - env.calls[index - 1].time, 60 * 0.9 ^ 10)
  end
end)

test("shared-quota consumption by other clients increases Lightroom's spacing", function()
  local client = httpClient()
  for _, remaining in ipairs({ 59, 12, 3 }) do
    enqueue(response(200, "[]", rateHeaders("60,60;w=60", remaining, 1)))
  end
  enqueue(response(200, "[]"))
  for _ = 1, 4 do
    assert(client.request("GET", "/artists.json"))
  end
  near(env.calls[3].time - env.calls[2].time, 60 * 0.8 ^ 10)
  near(env.calls[4].time - env.calls[3].time, 60 * 0.95 ^ 10)
end)

test("an exhausted quota blocks requests until reset, including after cookie reset", function()
  local client = httpClient()
  enqueue(response(200, "[]", rateHeaders("60,60;w=60", 0, 45)), response(200, "[]"))
  assert(client.request("GET", "/artists.json"))
  client.reset()
  assert(client.request("GET", "/artists.json"))
  near(env.calls[2].time, 45)
end)

test("zero capacity waits for the policy window without dividing by zero", function()
  local client = httpClient()
  enqueue(response(200, "[]", rateHeaders("0,0;w=30", 1, 5)), response(200, "[]"))
  assert(client.request("GET", "/artists.json"))
  assert(client.request("GET", "/artists.json"))
  near(env.calls[2].time, 30)
end)

test("API network failures leave a reset window of silence before retrying", function()
  local client = httpClient()
  enqueue(response(0, nil, rateHeaders(nil, nil, 20)), response(200, "[]"))
  assert(client.request("GET", "/artists.json"))
  near(env.calls[2].time, 20)
end)

test("successful media updates maintain a minimum quarter-second pause", function()
  enqueue(response(200, '{"uuid":"m"}', rateHeaders("60,60;w=60", 59, 1)), response(200, '{"uuid":"m"}'))
  assert(env.api.updatePhoto("m", "site", {}))
  assert(env.api.updatePhoto("m", "site", {}))
  near(env.calls[2].time - env.calls[1].time, 0.25)
end)

test("responses from concurrent requests cannot shorten a Retry-After block", function()
  cooperativeTasks()
  local client = httpClient()
  local slow = response(200, "[]", rateHeaders("60,60;w=60", 60, 0))
  slow.inspect = function()
    coroutine.yield("receiving")
  end
  enqueue(
    slow,
    response(429, '{"error":"Wait"}', { { field = "Retry-After", value = "60" } }),
    response(200, "[]", rateHeaders("60,60;w=60", 60, 1)),
    response(200, "[]")
  )
  local function request()
    assert(client.request("GET", "/artists.json"))
  end
  local first, second, third = coroutine.create(request), coroutine.create(request), coroutine.create(request)
  assert(coroutine.resume(first))
  env.now = 1
  -- A second request receives 429 before the first one's stale quota arrives.
  assert(coroutine.resume(second))
  assert(coroutine.resume(first))
  assert(coroutine.resume(third))
  equal(#env.calls, 2)
  equal(coroutine.status(third), "suspended")
  -- Both blocked requests may proceed only after the full cooldown.
  env.now = 61
  assert(coroutine.resume(second))
  assert(coroutine.resume(third))
  equal(coroutine.status(second), "dead")
  equal(coroutine.status(third), "dead")
  equal(#env.calls, 4)
  assert(env.calls[3].time >= 61 and env.calls[4].time >= 61)
end)

section("Discovery, pagination and caching")

test("lists follow exact next URLs, ignore monitor/count hints, and deduplicate", function()
  local nextURL = "/websites/site/galleries.json?token=a%2bb,c&per_page=1&page=7"
  enqueue(
    response(200, '[{"uuid":"g","name":"Old"}]', {
      { field = "Link", value = "<" .. nextURL .. '>; rel="next", </jobs/requests/r.json>; rel="monitor"' },
      { field = "X-Total-Pages", value = "999" },
    }),
    response(200, '[{"uuid":"g","name":"New"},{"uuid":"second","name":"Second"}]')
  )
  local galleries = assert(env.api.galleries("site"))
  equal(count(galleries), 2)
  equal(galleries.g.name, "New")
  equal(env.calls[2].url, apiOrigin .. nextURL)
  contains(env.signatures[2], "token=a%2bb,c&per_page=1&page=7\n")
  equal(#env.calls, 2)
end)

test("relative next links resolve against the API origin without re-encoding", function()
  enqueue(response(200, '[{"uuid":"g"}]', { { field = "Link", value = '<websites/site/galleries.json?token=a%2bb&page=2>; rel="next"' } }), response(200, "[]"))
  assert(env.api.galleries("site"))
  equal(env.calls[2].url, apiOrigin .. "/websites/site/galleries.json?token=a%2bb&page=2")
end)

test("empty pages with next links are traversed; empty lists stay valid", function()
  enqueue(response(200, "[]", { { field = "Link", value = "</websites/site/galleries.json?page=2>; rel=next" } }), response(200, "[]"))
  equal(count(assert(env.api.galleries("site"))), 0)
  equal(#env.calls, 2)
end)

test("all supporting lists paginate and retain UI lookup keys", function()
  enqueue(
    response(200, '[{"uuid":"w","urlname":"one","title":"One"}]', { { field = "Link", value = '</websites.json?page=2>; rel="next"' } }),
    response(200, '[{"uuid":"w2","urlname":"two","title":"Two"}]')
  )
  equal(assert(env.api.websites()).two.uuid, "w2")
  enqueue(
    response(200, '[{"uuid":"a","name":"A"}]', { { field = "Link", value = '</artists.json?page=2>; rel="next"' } }),
    response(200, '[{"uuid":"b","name":"B"}]')
  )
  equal(assert(env.api.artists()).b.name, "B")
  enqueue(
    response(200, '[{"uuid":"wm","file_name":"watermark.png"}]', { { field = "Link", value = '</artists/a/watermarks.json?page=2>; rel="next"' } }),
    response(200, '[{"uuid":"text","text":"Copyright","file_name":null}]')
  )
  local watermarks = assert(env.api.watermarks("a"))
  contains(watermarks.wm.name, "watermark.png")
  contains(watermarks.text.name, "Copyright")
  assert(watermarks[""] and watermarks.none)
  enqueue(
    response(200, '[{"uuid":"s","name":"Style"}]', { { field = "Link", value = '</websites/one/gallery_display_styles.json?page=2>; rel="next"' } }),
    response(200, '[{"uuid":"s2","name":"Style 2"}]')
  )
  equal(assert(env.api.galleryDisplayStyles("one")).s2.name, "Style 2")
  equal(#env.calls, 8)
end)

test("website structural details are lazy and cached", function()
  enqueue(response(200, '[{"uuid":"w","urlname":"site","hostname":"example.com","title":"Site"}]'))
  equal(assert(env.api.website("site")).uuid, "w")
  equal(#env.calls, 1)
  equal(assert(env.api.website("site")).home_url, nil)
  enqueue(response(200, '{"uuid":"w","urlname":"site","root_gallery_uuid":"root","home_url":"https://example.com/fr"}'))
  equal(assert(env.api.website("site", true)).root_gallery_uuid, "root")
  equal(assert(env.api.website("site", true)).home_url, "https://example.com/fr")
  equal(assert(env.api.website("site")).uuid, "w")
  equal(#env.calls, 2)
end)

test("concurrent first use of website details shares one request", function()
  cooperativeTasks()
  local item = response(200, '{"uuid":"w","home_url":"https://example.com","root_gallery_uuid":"root"}')
  item.inspect = function()
    coroutine.yield("receiving")
  end
  enqueue(item)
  local firstValue, secondValue
  local first = coroutine.create(function()
    firstValue = assert(env.api.website("site", true))
  end)
  local second = coroutine.create(function()
    secondValue = assert(env.api.website("site", true))
  end)
  -- The second caller waits while the first HTTP response is still pending.
  assert(coroutine.resume(first))
  assert(coroutine.resume(second))
  assert(coroutine.resume(first))
  assert(coroutine.resume(second))
  equal(coroutine.status(first), "dead")
  equal(coroutine.status(second), "dead")
  equal(firstValue, secondValue)
  equal(#env.calls, 1)
end)

test("cache reset during a load does not cache resources from the previous account", function()
  local item = response(200, '{"uuid":"old-site","home_url":"https://old.example.com"}')
  item.inspect = function()
    env.api.connect("test-key", "test-secret", "other@example.com", "other-password")
  end
  enqueue(item, response(200, '{"uuid":"new-site","home_url":"https://new.example.com"}'))
  equal(assert(env.api.website("site", true)).uuid, "old-site")
  equal(assert(env.api.website("site", true)).uuid, "new-site")
  equal(assert(env.api.website("site", true)).uuid, "new-site")
  equal(#env.calls, 2)
end)

test("empty discovery and authorization failures do not mark authenticated sessions logged out", function()
  env.api.loggedin = true
  enqueue(response(200, "[]"), response(403, '{"error":"No rights"}'))
  local _, message = env.api.artists()
  assert(message)
  equal(env.api.loggedin, true)
  _, message = env.api.galleryDisplayStyles("site")
  equal(message, "No rights")
  equal(env.api.loggedin, true)
end)

test("tree pages need not repeat ancestors and only direct children are returned", function()
  enqueue(
    response(200, '[{"uuid":"ancestor","parent_uuid":null},{"uuid":"parent","parent_uuid":"ancestor"}]', {
      { field = "Link", value = '</websites/site/galleries/parent/subgalleries.json?filter%5Bname%5D=Caf%C3%A9&page=2>; rel="next"' },
    }),
    response(200, '[{"uuid":"child","parent_uuid":"parent","name":"Café","subgalleries_count":0}]')
  )
  local galleries = assert(env.api.subGalleriesInGallery("site", "parent", "Café"))
  equal(count(galleries), 1)
  equal(galleries.child.name, "Café")
end)

test("included gallery media UUIDs paginate and deduplicate", function()
  enqueue(
    response(
      200,
      '{"uuid":"g","media_uuids":["one","two"]}',
      { { field = "Link", value = '</websites/site/galleries/g.json?include=media_uuids&page=2>; rel="next"' } }
    ),
    response(200, '{"uuid":"g","media_uuids":["two","three"]}')
  )
  equal(count(assert(env.api.photosInGallery("site", "g"))), 3)
  equal(#env.calls, 2)
end)

section("Galleries and Lightroom collections")

test("gallery mutations use canonical fields and rename/move omit cached settings", function()
  enqueue(response(201, '{"uuid":"g"}'), response(200, '{"uuid":"g"}'), response(200, '{"uuid":"g"}'), response(204, ""))
  local info = { name = "Name", collectionSettings = { description = "Introduction", display_style = "style" } }
  assert(env.api.createGallery("site", "parent", info))
  local created = payload(env.calls[1]).gallery
  equal(created.name, "Name")
  equal(created.parent_uuid, "parent")
  equal(created.description, "Introduction")
  equal(created.gallery_display_style_uuid, "style")
  assert(env.api.updateGallery("site", "g", "new-parent", info))
  local updated = payload(env.calls[2]).gallery
  equal(updated.parent_uuid, "new-parent")
  equal(updated.description, nil)
  equal(updated.gallery_display_style_uuid, nil)
  assert(env.api.reorderGallery("site", "g", { "third", "first", "second" }))
  equal(payload(env.calls[3]).gallery.media_uuids_order, "third,first,second")
  local _, message = env.api.deleteGallery("site", "g")
  equal(message, nil)
  equal(#env.calls, 4)
end)

local function catalogCollection(name, id, kind, settings)
  local item = { name = name, id = id, kind = kind or "LrPublishedCollection", settings = settings or {}, children = {}, sets = {}, localIdentifier = 1 }
  function item:getName()
    return self.name
  end
  function item:getRemoteId()
    return self.id
  end
  function item:setRemoteId(value)
    self.id = value
  end
  function item:getRemoteUrl()
    return self.url
  end
  function item:setRemoteUrl(value)
    self.url = value
  end
  function item:type()
    return self.kind
  end
  function item:getChildCollections()
    return self.children
  end
  function item:getChildCollectionSets()
    return self.sets
  end
  function item:getCollectionInfoSummary()
    return { collectionSettings = self.settings }
  end
  item.getCollectionSetInfoSummary = item.getCollectionInfoSummary
  function item:setCollectionSettings(value)
    self.settings = value
  end
  item.setCollectionSetSettings = item.setCollectionSettings
  function item:delete()
    self.deleted = true
  end
  return item
end
local function catalogService()
  local catalog = {
    withWriteAccessDo = function(_, _, action)
      action()
    end,
    withPrivateWriteAccessDo = function(_, action)
      action()
    end,
  }
  local service = catalogCollection("service", nil, "LrPublishService")
  service.catalog = catalog
  function service:createPublishedCollection(name, parent)
    local item = catalogCollection(name)
    item.catalog = catalog
    parent.children[#parent.children + 1] = item
    return item
  end
  function service:createPublishedCollectionSet(name, parent)
    local item = catalogCollection(name, nil, "LrPublishedCollectionSet")
    item.catalog = catalog
    parent.sets[#parent.sets + 1] = item
    return item
  end
  return service
end
local function structureResponses()
  enqueue(
    response(200, '{"uuid":"w","urlname":"site","root_gallery_uuid":"root","home_url":"https://example.com/fr/"}'),
    response(
      200,
      '[{"uuid":"root","name":"Root","parent_uuid":null,"full_url_path":"","medias_count":0},{"uuid":"folder","name":"Folder","parent_uuid":"root","full_url_path":"folder","medias_count":0},{"uuid":"leaf","name":"Leaf","parent_uuid":"folder","full_url_path":"folder/leaf","medias_count":1}]'
    )
  )
end
local progress = {
  setCaption = function() end,
  isCanceled = function()
    return false
  end,
}

test("gallery import creates hierarchy using only site details and basic list", function()
  local service = catalogService()
  structureResponses()
  local result = assert(env.api.synchronizeGalleries("site", service, progress))
  equal(result.created, 2)
  equal(service.sets[1].id, "folder")
  equal(service.sets[1].children[1].id, "leaf")
  equal(service.sets[1].children[1].url, "https://example.com/fr/-/folder/leaf")
  equal(service.sets[1].children[1].settings.description, nil)
  equal(#env.calls, 2)
end)

test("structural synchronization preserves existing descriptions and styles", function()
  local service = catalogService()
  local folder = service:createPublishedCollectionSet("Folder", service)
  folder.id, folder.settings = "folder", { description = "Cached folder introduction", display_style = "folder-style" }
  local leaf = service:createPublishedCollection("Leaf", folder)
  leaf.id, leaf.settings = "leaf", { description = "Cached leaf introduction", display_style = "leaf-style" }
  structureResponses()
  assert(env.api.synchronizeGalleries("site", service, progress))
  equal(folder.settings.description, "Cached folder introduction")
  equal(leaf.settings.display_style, "leaf-style")
  equal(#env.calls, 2)
  enqueue(response(200, '[{"uuid":"root","name":"Root","parent_uuid":null,"full_url_path":"","medias_count":0}]'))
  assert(env.api.synchronizeGalleries("site", service, progress))
  equal(#env.calls, 3) -- website details are reused
end)

test("explicit gallery editing can clear the introduction", function()
  local service = catalogService()
  local collection = service:createPublishedCollection("Name", service)
  collection.id = "g"
  enqueue(
    response(200, '{"uuid":"w","root_gallery_uuid":"root","home_url":"https://example.com"}'),
    response(200, '{"uuid":"g","name":"Name","parent_uuid":"root","full_url_path":"name","description":"Old","gallery_display_style_uuid":"style"}'),
    response(200, '{"uuid":"g","name":"Name","parent_uuid":"root","full_url_path":"name","description":"","gallery_display_style_uuid":"style"}')
  )
  assert(
    env.api.createOrUpdateGallery(
      "site",
      { name = "Name", parents = {}, publishedCollection = collection, collectionSettings = { description = "", display_style = "style" } },
      true
    )
  )
  equal(payload(env.calls[3]).gallery.description, "")
  equal(collection.settings.description, "")
  equal(#env.calls, 3)
end)

test("failed synchronization pages never mutate existing Lightroom structure", function()
  local service = catalogService()
  local existing = service:createPublishedCollection("Existing", service)
  existing.id = "existing"
  enqueue(
    response(200, '{"uuid":"w","home_url":"https://example.com","root_gallery_uuid":"root"}'),
    response(200, '[{"uuid":"root","name":"Root","full_url_path":""}]', {
      { field = "Link", value = '</websites/site/galleries.json?page=2>; rel="next"' },
    }),
    response(403, '{"error":"Denied"}')
  )
  local result, message = env.api.synchronizeGalleries("site", service, progress)
  equal(result, nil)
  equal(message, "Denied")
  equal(existing.deleted, nil)
  equal(env.api.canSynchronize, true)
end)

test("failed gallery reads do not trigger accidental gallery creation", function()
  local service = catalogService()
  local collection = service:createPublishedCollection("Name", service)
  collection.id = "g"
  enqueue(response(200, '{"uuid":"w","home_url":"https://example.com","root_gallery_uuid":"root"}'), response(403, '{"error":"Denied"}'))
  local result, message = env.api.createOrUpdateGallery("site", { name = "Name", parents = {}, publishedCollection = collection })
  equal(result, nil)
  equal(message, "Denied")
  equal(#env.calls, 2)
end)

test("gallery settings load live details on demand", function()
  enqueue(response(200, '[{"uuid":"live-style","name":"Style"}]'))
  assert(env.api.galleryDisplayStyles("site"))
  local collection = catalogCollection("Name", "g")
  local settings = { description = "Stale", display_style = "stale-style", addObserver = function() end }
  enqueue(response(200, '{"uuid":"g","name":"Live name","description":"Live introduction","gallery_display_style_uuid":"live-style"}'))
  local dialogs = require("PhotoDeckDialogs")
  dialogs.viewForCollectionSettings(env.modules.LrView.osFactory(), {
    apiKey = "test-key",
    apiSecret = "test-secret",
    username = "user@example.com",
    password = "test-password",
    websiteChosen = "site",
  }, { publishedCollection = collection, collectionSettings = settings })
  equal(settings.description, "Live introduction")
  equal(settings.display_style, "live-style")
  equal(settings.LR_liveName, "Live name")
  equal(#env.calls, 2) -- cached style list, one fresh detail request
end)

section("Media updates and batch selections")

local function mediaIds(size)
  local ids = {}
  for index = 1, size do
    ids[index] = "m-" .. index
  end
  return ids
end

test("metadata-only republish combines publication and all metadata in one PUT", function()
  local photo = {}
  local metadata = {
    title = "Title",
    caption = "Caption",
    keywordTagsForExport = "one, two",
    location = "Place",
    city = "City",
    stateProvince = "State",
    country = "Country",
    creator = "Author",
    copyright = "Copyright",
    locationShown = { { WorldRegion = "Region" } },
  }
  function photo:getFormattedMetadata(key)
    return metadata[key]
  end
  function photo:getRawMetadata(key)
    if key == "dateTimeOriginalISO8601" then
      return "2026-10-15T12:00:00Z"
    end
  end
  enqueue(response(200, '{"uuid":"m","file_name":"photo.jpg"}'))
  assert(env.api.updatePhoto("m", "site", { injectMetadata = true, lrPhoto = photo, publishToGallery = "g" }))
  equal(env.calls[1].method, "PUT")
  local media = payload(env.calls[1]).media
  for name, expected in pairs({
    title = "Title",
    description = "Caption",
    keywords = "one, two",
    location = "Place",
    city = "City",
    state = "State",
    country = "Country",
    author = "Author",
    copyright = "Copyright",
    region = "Region",
    delete_artist_rating = true,
    date_created = "2026-10-15T12:00:00Z",
  }) do
    equal(media[name], expected, "media." .. name)
  end
  equal(media.publish_to_gallery_uuids[1], "g")
  equal(media.content, nil)
  equal(media.artist_rating, nil)
  equal(#env.calls, 1)
end)

test("Lightroom rating strings are sent as JSON numbers", function()
  local photo = {
    getFormattedMetadata = function(_, key)
      if key == "rating" then return "4" end
    end,
    getRawMetadata = function() end,
  }
  enqueue(response(200, '{"uuid":"m"}'))
  assert(env.api.updatePhoto("m", "site", { injectMetadata = true, lrPhoto = photo }))
  local media = payload(env.calls[1]).media
  equal(media.artist_rating, 4)
  equal(media.delete_artist_rating, nil)
end)

test("single unpublication is synchronous and incremental", function()
  enqueue(response(200, '{"uuid":"m","gallery_uuids":["other"]}'))
  assert(env.api.unpublishPhoto("m", "gallery"))
  local media = payload(env.calls[1]).media
  equal(media.unpublish_from_gallery_uuids[1], "gallery")
  equal(media.gallery_uuids, nil)
  equal(#env.calls, 1)
end)

test("batch selections accept the 3000-item boundary", function()
  enqueue(response(202, '{"notice":"Queued"}'))
  assert(env.api.deletePhotos(mediaIds(3000)))
  equal(#env.calls, 1)
end)

test("batch selections split at 3000 and acknowledge accepted chunks", function()
  local ids, accepted = mediaIds(6001), {}
  for _ = 1, 3 do
    enqueue(response(202, '{"notice":"Queued"}', { { field = "Link", value = '</jobs/requests/r.json>; rel="monitor"' } }))
  end
  assert(env.api.unpublishPhotos(ids, "gallery", function(id)
    accepted[#accepted + 1] = id
  end))
  equal(#env.calls, 3)
  for index, call in ipairs(env.calls) do
    equal(call.method, "PATCH")
    equal(call.url, apiOrigin .. "/medias.json")
    contains(env.signatures[index], "PATCH\n/medias.json\n\n")
  end
  equal(#accepted, 6001)
  for index, id in ipairs(ids) do
    equal(accepted[index], id, "Acknowledged media " .. index)
  end
  equal(payload(env.calls[1]).medias.unpublish_from_gallery_uuids[1], "gallery")
  equal(payload(env.calls[3]).select, "m-6001")
  local chunkCount = 0
  for _ in payload(env.calls[1]).select:gmatch("[^,]+") do
    chunkCount = chunkCount + 1
  end
  equal(chunkCount, 3000)
  equal(payload(env.calls[1]).medias.on, nil)
end)

test("partial batch failure only acknowledges accepted IDs", function()
  local ids, accepted = mediaIds(3001), 0
  enqueue(response(202, '{"notice":"Queued"}'), response(422, '{"error":"Denied"}'))
  local value, message = env.api.deletePhotos(ids, function()
    accepted = accepted + 1
  end)
  equal(value, nil)
  equal(message, "Denied")
  equal(accepted, 3000)
  equal(payload(env.calls[1]).medias.delete, true)
  equal(#env.calls, 2)
  for index, call in ipairs(env.calls) do
    equal(call.method, "PATCH")
    equal(call.url, apiOrigin .. "/medias.json")
    contains(env.signatures[index], "PATCH\n/medias.json\n\n")
  end
end)

test("empty/deduplicated batch selections are safe and never select all", function()
  assert(env.api.deletePhotos({}))
  equal(#env.calls, 0)
  for _, ids in ipairs({ { "all" }, { "m", "bad,id" }, { "m", "" } }) do
    local value, message = env.api.deletePhotos(ids)
    equal(value, nil)
    assert(message)
  end
  equal(#env.calls, 0)
  enqueue(response(202, '{"notice":"Queued"}'))
  assert(env.api.deletePhotos({ "m", "m", "second" }))
  equal(payload(env.calls[1]).select, "m,second")
  equal(#env.calls, 1)
end)

section("Uploads and recovery")

test("raw upload transfers bytes to signed storage and confirms completion", function()
  enqueue(response(201, instructions()), response(200, "", {}, "text/plain"), response(200, '{"uuid":"m","file_name":"returned.jpg","processed":false}'))
  local media = assert(env.api.uploadPhoto("site", uploadAttributes()))
  equal(media.file_name, "returned.jpg")
  equal(media.processed, false)
  equal(#env.calls, 3)
  local requested = payload(env.calls[1])
  equal(requested.artist_uuid, "library")
  equal(requested.media.artist_uuid, nil)
  equal(requested.media.watermark_uuid, "none")
  equal(requested.media.publish_to_gallery_uuids[1], "gallery")
  equal(requested.media.content.upload_location, "REQUEST")
  equal(requested.media.content.file_name, "upload.bin")
  equal(requested.media.content.file_size, #env.calls[2].body)
  equal(requested.media.content.mime_type, "image/tiff")
  equal(requested.media.content.capabilities, "raw")
  equal(env.calls[2].url, "https://storage.example.test/file?secret=signed-value")
  equal(env.calls[2].method, "PUT")
  equal(header(env.calls[2], "X-PhotoDeck-Authorization"), nil)
  equal(header(env.calls[2], "Authorization"), nil)
  equal(header(env.calls[2], "Cookie"), "")
  equal(header(env.calls[2], "Content-Type"), "image/jpeg")
  equal(tonumber(header(env.calls[2], "Content-Length")), #env.calls[2].body)
  local confirmed = payload(env.calls[3]).media.content
  equal(confirmed.upload_location, "primary")
  equal(confirmed.file_name, "returned.jpg")
  equal(confirmed.mime_type, "image/jpeg")
  equal(confirmed.file_size, #env.calls[2].body)
  equal(type(confirmed.upload_duration), "number")
  equal(#env.signatures, 2)
  for _, line in ipairs(env.logs) do
    absent(line, "test-password")
    absent(line, "test-secret")
    absent(line, "signed-value")
    absent(line, "storage-policy")
  end
end)

test("known UUID replacement keeps PUT and multipart storage honors returned method", function()
  enqueue(
    response(200, instructions({ multipart = true, method = "PUT" })),
    response(204, "", {}, "text/plain"),
    response(200, '{"uuid":"m","file_name":"returned.jpg"}')
  )
  assert(env.api.updatePhoto("m", "site", uploadAttributes()))
  equal(env.calls[1].method, "PUT")
  equal(env.calls[2].method, "PUT")
  local fileIndex = assert(env.calls[2].body:find('name="file"', 1, true))
  assert(env.calls[2].body:find('name="policy"', 1, true) < fileIndex)
  assert(env.calls[2].body:find('name="key"', 1, true) < fileIndex)
  contains(env.calls[2].body, 'filename="returned.jpg"')
  contains(env.calls[2].body, "PhotoDeck upload fixture")
  equal(#env.calls, 3)
end)

for _, case in ipairs({
  { name = "missing", body = '{"uuid":"m"}' },
  { name = "null", body = '{"uuid":"m","upload_url":null}' },
  { name = "empty", body = '{"uuid":"m","upload_url":""}' },
}) do
  test(case.name .. " upload URLs fail before transferring file bytes", function()
    enqueue(response(201, case.body))
    local media, message, stop = env.api.uploadPhoto("site", uploadAttributes())
    equal(media, nil)
    equal(message, "Upload URL missing")
    equal(stop, false)
    equal(#env.calls, 1)
    equal(payload(env.calls[1]).media.content.upload_location, "REQUEST")

    enqueue(response(201, instructions()), response(200, "", {}, "text/plain"), response(200, '{"uuid":"m"}'))
    assert(env.api.uploadPhoto("site", uploadAttributes()))
    equal(payload(env.calls[2]).media.content.upload_location, "REQUEST")
    equal(env.calls[3].url, "https://storage.example.test/file?secret=signed-value")
    equal(#env.calls, 4)
  end)
end

for _, case in ipairs({
  { name = "creation", method = "POST" },
  { name = "replacement", method = "PUT", photoId = "known" },
}) do
  test("422 upload instructions reject " .. case.name .. " without sending file bytes", function()
    enqueue(response(422, '{"error":"No upload URL available"}'))
    local media, message, stop = env.api.updatePhoto(case.photoId, "site", uploadAttributes())
    equal(media, nil)
    equal(message, "No upload URL available")
    equal(stop, false)
    equal(#env.calls, 1)
    equal(env.calls[1].method, case.method)
    equal(payload(env.calls[1]).media.content.upload_location, "REQUEST")
  end)
end

test("422 metadata updates report validation errors without retrying", function()
  enqueue(response(422, '{"error":"Invalid title"}'))
  local media, message, stop = env.api.updatePhoto("known", "site", { publishToGallery = "gallery" })
  equal(media, nil)
  equal(message, "Invalid title")
  equal(stop, false)
  equal(#env.calls, 1)
  equal(payload(env.calls[1]).media.content, nil)
end)

test("422 completion failures never resend file bytes", function()
  enqueue(
    response(201, instructions()),
    response(200, "", {}, "text/plain"),
    response(422, '{"error":"Invalid upload confirmation"}')
  )
  local media, message, stop = env.api.uploadPhoto("site", uploadAttributes())
  equal(media, nil)
  equal(message, "Invalid upload confirmation")
  equal(stop, false)
  equal(#env.calls, 3)
  equal(payload(env.calls[3]).media.content.upload_location, "primary")
end)

test("upload retries exclude failed locations and reuse the media UUID", function()
  enqueue(response(201, instructions()))
  for _ = 1, 3 do
    enqueue(response(503, "storage unavailable", {}, "text/plain"))
  end
  enqueue(response(200, instructions({ location = "secondary" })), response(200, "", {}, "text/plain"), response(200, '{"uuid":"m"}'))
  assert(env.api.uploadPhoto("site", uploadAttributes()))
  equal(env.calls[5].method, "PUT")
  contains(env.calls[5].url, "/medias/m.json")
  equal(payload(env.calls[5]).media.content.failed_locations, "primary")
  equal(#env.calls, 7)
end)

test("expired instructions refresh without replaying creation", function()
  enqueue(
    response(201, instructions({ expiry = 1 })),
    response(503, "retry", {}, "text/plain"),
    response(200, instructions({ location = "secondary" })),
    response(200, "", {}, "text/plain"),
    response(200, '{"uuid":"m"}')
  )
  assert(env.api.uploadPhoto("site", uploadAttributes()))
  equal(env.calls[3].method, "PUT")
  equal(#env.calls, 5)
end)

test("stop_with_error propagates from instruction and completion failures", function()
  enqueue(response(422, '{"error":"Upload quota","stop_with_error":true}'))
  local media, message, stop = env.api.uploadPhoto("site", uploadAttributes())
  equal(media, nil)
  equal(message, "Upload quota")
  equal(stop, true)
  enqueue(response(201, instructions()), response(200, "", {}, "text/plain"), response(422, '{"error":"Stop queue","stop_with_error":true}'))
  media, message, stop = env.api.uploadPhoto("site", uploadAttributes())
  equal(media, nil)
  equal(message, "Stop queue")
  equal(stop, true)
  equal(#env.calls, 4)
end)

test("ambiguous completion failure never reuploads bytes", function()
  enqueue(response(201, instructions()), response(200, "", {}, "text/plain"), response(0, nil))
  local media, message = env.api.uploadPhoto("site", uploadAttributes())
  equal(media, nil)
  assert(message)
  equal(#env.calls, 3)
end)

test("missing media signals recovery and missing local files fail before upload", function()
  enqueue(response(404, '{"error":"Missing"}'))
  local media = env.api.updatePhoto("missing", "site", {}, true)
  equal(media.notfound, true)
  local _, message = env.api.uploadPhoto("site", { contentPath = "tests/fixtures/does-not-exist" })
  assert(message)
  equal(#env.calls, 1)
end)

test("partial storage responses are not treated as completed transfers", function()
  enqueue(response(201, instructions()))
  local interrupted = response(200, "", {}, "text/plain")
  interrupted.error = { errorCode = "networkConnectionLost" }
  enqueue(interrupted, response(200, "", {}, "text/plain"), response(200, '{"uuid":"m"}'))
  assert(env.api.uploadPhoto("site", uploadAttributes()))
  equal(env.calls[2].url, env.calls[3].url)
  contains(env.calls[4].url, "/medias/m.json")
  equal(#env.calls, 4)
end)

section("Storage streams and diagnostics")

local function uploadFile(data)
  local path = "virtual-upload.bin"
  local stats = { opened = 0, closed = 0 }
  local originalAttributes = env.modules.LrFileUtils.fileAttributes
  env.modules.LrFileUtils.fileAttributes = function(name)
    if name == path then
      return { fileSize = #data }
    end
    return originalAttributes(name)
  end
  _G.io = setmetatable({
    open = function(name, mode)
      if name ~= path then
        return realIO.open(name, mode)
      end
      assert(not env.inBodyGetter, "Opening a file inside the native byte-getter callback")
      stats.opened = stats.opened + 1
      local position, closed = 1, false
      return {
        read = function(_, size)
          assert(not closed)
          if stats.readError then
            return nil, "simulated read failure"
          end
          if position > #data then
            return nil
          end
          local result = size == "*a" and data:sub(position) or data:sub(position, position + size - 1)
          position = position + #result
          return result
        end,
        close = function()
          assert(not env.inBodyGetter, "Closing a file inside the native byte-getter callback")
          assert(not closed, "File closed twice")
          closed = true
          stats.closed = stats.closed + 1
        end,
      }
    end,
  }, { __index = realIO })
  return path, stats
end

test("multi-chunk binary PUT opens/closes files outside the native callback", function()
  local bytes = string.rep("\000\255AB\r\n", 554751):sub(1, 3328502)
  equal(#bytes, 3328502)
  local path, stats = uploadFile(bytes)
  local attributes = uploadAttributes()
  attributes.contentPath = path
  enqueue(response(201, instructions()), response(200, "", {}, "text/plain"), response(200, '{"uuid":"m"}'))
  local media, message = env.api.uploadPhoto("site", attributes)
  assert(media, message)
  equal(env.calls[2].body, bytes)
  equal(#env.calls[2].chunks, 4)
  equal(stats.opened, 1)
  equal(stats.closed, 1)
  equal(header(env.calls[2], "Authorization"), nil)
  equal(#env.calls, 3)
end)

test("multipart POST uses the SDK with signing fields before the file", function()
  local bytes = "\000\255Swift upload\r\n"
  local path, stats = uploadFile(bytes)
  local attributes = uploadAttributes()
  attributes.contentPath = path
  local params =
    '{"expires":1793216530,"max_file_size":3328502,"max_file_count":1,"x_delete_after":2629746,"content-type":"image/jpeg","cache-control":"no-cache","signature":"private-form-signature"}'
  enqueue(
    response(201, instructions({ multipart = true, method = "POST", params = params })),
    response(201, "", {}, "text/plain"),
    response(200, '{"uuid":"m"}')
  )
  local media, message = env.api.uploadPhoto("site", attributes)
  assert(media, message)
  equal(env.calls[2].transport, "postMultipart")
  equal(env.calls[2].method, "POST")
  equal(header(env.calls[2], "Authorization"), nil)
  equal(header(env.calls[2], "X-PhotoDeck-Authorization"), nil)
  equal(header(env.nativeMultipart, "Content-Type"), nil) -- the SDK owns the boundary
  equal(header(env.nativeMultipart, "Content-Length"), nil)
  local parts = env.nativeMultipart.params
  equal(#parts, 8)
  for index = 1, 7 do
    equal(type(parts[index].value), "string")
    equal(parts[index].filePath, nil)
  end
  equal(parts[8].fileName, "returned.jpg")
  contains(env.calls[2].body, bytes)
  equal(stats.opened, 1)
  equal(stats.closed, 1)
  equal(#env.calls, 3)
  for _, line in ipairs(env.logs) do
    absent(line, "private-form-signature")
  end
end)

test("custom multipart PUT also closes file handles outside the native callback", function()
  local path, stats = uploadFile(string.rep("binary\000", 160000))
  local attributes = uploadAttributes()
  attributes.contentPath = path
  enqueue(response(201, instructions({ multipart = true })), response(200, "", {}, "text/plain"), response(200, '{"uuid":"m"}'))
  local media, message = env.api.uploadPhoto("site", attributes)
  assert(media, message)
  equal(stats.opened, stats.closed)
  equal(stats.closed, 1)
  equal(env.calls[2].method, "PUT")
end)

test("storage diagnostics report native error codes but never signed URLs/messages", function()
  local client = httpClient()
  local json = require("PhotoDeckJSON")
  local media = json.decode(instructions())
  media.upload_file_param = nil
  local item = response(0, nil)
  item.error = { errorCode = "cannotConnectToHost", nativeCode = -1004, name = "Failed https://storage.example.test/file?secret=signed-value" }
  enqueue(item)
  local size = env.modules.LrFileUtils.fileAttributes(fixture).fileSize
  local success, message, retryable = client.transfer(media, fixture, size)
  equal(success, nil)
  assert(message)
  equal(retryable, true)
  local logs = table.concat(env.logs, "\n")
  contains(logs, "HTTP 0")
  contains(logs, "network=cannotConnectToHost")
  contains(logs, "native=-1004")
  absent(logs, "signed-value")
  absent(logs, "https://")
end)

test("storage SDK exceptions close streams and retain a safe diagnostic category", function()
  local path, stats = uploadFile("payload")
  local client = httpClient()
  local media = require("PhotoDeckJSON").decode(instructions())
  media.upload_file_param = nil
  env.modules.LrHttp.post = function()
    error("attempt to yield: https://storage.example.test/file?secret=signed-value")
  end
  local success = client.transfer(media, path, 7)
  equal(success, nil)
  equal(stats.opened, 1)
  equal(stats.closed, 1)
  local logs = table.concat(env.logs, "\n")
  contains(logs, "network=requestFailed")
  contains(logs, "exception=yieldNotAllowed")
  absent(logs, "signed-value")
end)

test("file read errors close streams and are not retried as storage outages", function()
  local path, stats = uploadFile("payload")
  stats.readError = true
  local attributes = uploadAttributes()
  attributes.contentPath = path
  enqueue(response(201, instructions()))
  local media, message = env.api.uploadPhoto("site", attributes)
  equal(media, nil)
  contains(message, "reading file")
  equal(stats.opened, 1)
  equal(stats.closed, 1)
  equal(#env.calls, 1)
  contains(table.concat(env.logs, "\n"), "network=fileReadError")
end)

section("Publish and export integration")

local function runPublish(options)
  options = options or {}
  local service = catalogService()
  service.localIdentifier = 10
  local photo = { properties = options.properties or {}, localIdentifier = 42, collections = options.collections or {} }
  function photo:getPropertyForPlugin(_, key)
    return self.properties[key]
  end
  function photo:setPropertyForPlugin(_, key, value)
    self.properties[key] = value
  end
  function photo:getRawMetadata(key)
    if key == "isVirtualCopy" then
      return options.virtualCopy
    end
  end
  function photo:getFormattedMetadata()
    return nil
  end
  function photo:getContainedPublishedCollections()
    return self.collections
  end
  local rendition = { photo = photo, publishedPhotoId = options.publishedPhotoId }
  function rendition:waitForRender()
    return true, fixture
  end
  function rendition:recordPublishedPhotoId(value)
    self.recordedId = value
  end
  function rendition:uploadFailed(message)
    self.failure = message
  end
  local exportContext = {
    propertyTable = {
      apiKey = "test-key",
      apiSecret = "test-secret",
      username = "user@example.com",
      password = "test-password",
      websiteChosen = "site",
      LR_isExportForPublish = options.publish,
      LR_format = "JPEG",
      artistChosen = "library",
      updateMetadataOnRepublish = true,
    },
    exportSession = {
      catalog = service.catalog,
      countRenditions = function()
        return 1
      end,
    },
    publishService = service,
    publishedCollectionInfo = { remoteId = "g" },
    configureProgress = function()
      return {
        setPortionComplete = function() end,
        isCanceled = function()
          return false
        end,
        done = function() end,
      }
    end,
    renditions = function()
      local yielded = false
      return function()
        if not yielded then
          yielded = true
          return 1, rendition
        end
      end
    end,
  }
  require("PhotoDeckPublishServiceProvider").processRenderedPhotos(nil, exportContext)
  return rendition, photo
end

test("export-only provider uses JSON upload results without website discovery", function()
  enqueue(response(201, instructions()), response(200, "", {}, "text/plain"), response(200, '{"uuid":"m","file_name":"returned.jpg"}'))
  local rendition, photo = runPublish()
  equal(rendition.failure, nil)
  equal(photo.properties.fileName, "returned.jpg")
  equal(rendition.recordedId, nil)
  equal(#env.calls, 3)
end)

test("export provider reports 422 upload-instruction errors without retrying", function()
  enqueue(response(422, '{"error":"No upload URL available"}'))
  local rendition, photo = runPublish()
  equal(rendition.failure, "No upload URL available")
  equal(photo.properties.fileName, nil)
  equal(#env.calls, 1)
  equal(payload(env.calls[1]).media.content.upload_location, "REQUEST")
end)

test("publishing virtual copies preserves catalog schema and website-specific keys", function()
  enqueue(
    response(200, '[{"uuid":"w","urlname":"site"}]'),
    response(200, '{"uuid":"g"}'),
    response(201, instructions()),
    response(200, "", {}, "text/plain"),
    response(200, '{"uuid":"m","file_name":"returned.jpg"}')
  )
  local rendition, photo = runPublish({ publish = true, virtualCopy = true, properties = { photoId = "other-website:other-media " } })
  equal(rendition.failure, nil)
  equal(rendition.recordedId, "m")
  contains(photo.properties.photoId, "w/42:m ")
  contains(photo.properties.photoId, "other-website:other-media ")
  equal(#env.calls, 5) -- website summary, gallery read and three-step upload
end)

test("metadata republish reuses known media without upload or processing polls", function()
  enqueue(response(200, '[{"uuid":"w","urlname":"site"}]'), response(200, '{"uuid":"g"}'), response(200, '{"uuid":"known","file_name":"existing.jpg"}'))
  local rendition, photo = runPublish({ publish = true, properties = { photoId = "w:known " } })
  equal(rendition.failure, nil)
  equal(rendition.recordedId, "known")
  equal(photo.properties.fileName, "existing.jpg")
  equal(payload(env.calls[3]).media.content, nil)
  equal(payload(env.calls[3]).media.publish_to_gallery_uuids[1], "g")
  equal(#env.calls, 3)
end)

test("provider recreates remotely deleted media with one new three-step upload", function()
  enqueue(
    response(200, '[{"uuid":"w","urlname":"site"}]'),
    response(200, '{"uuid":"g"}'),
    response(404, '{"error":"Missing"}'),
    response(201, instructions()),
    response(200, "", {}, "text/plain"),
    response(200, '{"uuid":"m","file_name":"returned.jpg"}')
  )
  local rendition, photo = runPublish({ publish = true, properties = { photoId = "w:gone " }, publishedPhotoId = "gone" })
  equal(rendition.failure, nil)
  equal(rendition.recordedId, "m")
  equal(photo.properties.photoId, "w:m ")
  equal(env.calls[4].method, "POST")
  equal(#env.calls, 6)
end)

test("provider preserves per-chunk acknowledgments on batch failure", function()
  local ids, publishedPhotos, acknowledged = mediaIds(3001), {}, 0
  for index, id in ipairs(ids) do
    publishedPhotos[index] = {
      getRemoteId = function()
        return id
      end,
      getPhoto = function()
        return {
          getContainedPublishedCollections = function()
            return {}
          end,
        }
      end,
    }
  end
  env.catalog = {
    getPublishedCollectionByLocalIdentifier = function()
      return {
        getRemoteId = function()
          return "g"
        end,
        getPublishedPhotos = function()
          return publishedPhotos
        end,
      }
    end,
  }
  enqueue(response(202, '{"notice":"Queued"}'), response(422, '{"error":"Denied"}'))
  local ok, message = pcall(
    require("PhotoDeckPublishServiceProvider").deletePhotosFromPublishedCollection,
    {
      apiKey = "test-key",
      apiSecret = "test-secret",
      username = "user@example.com",
      password = "test-password",
    },
    ids,
    function()
      acknowledged = acknowledged + 1
    end,
    1
  )
  equal(ok, false)
  contains(message, "Denied")
  equal(acknowledged, 3000)
  equal(#env.calls, 2)
end)

section("Localization")

test("French and German translations cover active keys without stale entries", function()
  local function readText(path)
    local file = assert(realIO.open(path, "rb"))
    local text = assert(file:read("*a"))
    file:close()
    return text
  end
  local defaults = {}
  for _, path in ipairs({
    "Info.lua",
    "PhotoDeckAPI.lua",
    "PhotoDeckDialogs.lua",
    "PhotoDeckHttp.lua",
    "PhotoDeckJSON.lua",
    "PhotoDeckMetadataDefinition.lua",
    "PhotoDeckPublishServiceProvider.lua",
    "PhotoDeckUtils.lua",
  }) do
    for key, value in readText(path):gmatch('"(%$%$%$/[^="\r\n]+)=([^"\r\n]*)"') do
      assert(not defaults[key] or defaults[key] == value, "Conflicting default: " .. key)
      defaults[key] = value
    end
  end
  assert(next(defaults), "No localization keys found")
  local function placeholders(value)
    local counts = {}
    for token in value:gmatch("%^(%d+)") do
      counts[token] = (counts[token] or 0) + 1
    end
    for token in value:gmatch("%^([n.])") do
      counts[token] = (counts[token] or 0) + 1
    end
    return counts
  end
  for _, language in ipairs({ "fr", "de" }) do
    local path = "TranslatedStrings_" .. language .. ".txt"
    local translations = {}
    local text = readText(path):gsub("^\239\187\191", "")
    for line in text:gmatch("[^\r\n]+") do
      local key, value = line:match('^"(%$%$%$/[^="]+)=([^"]*)"$')
      assert(key, "Invalid translation entry in " .. path .. ": " .. line)
      assert(not translations[key], "Duplicate key in " .. path .. ": " .. key)
      assert(defaults[key], "Stale key in " .. path .. ": " .. key)
      assert(value ~= "", "Empty translation in " .. path .. ": " .. key)
      translations[key] = value
      local expected, actual = placeholders(defaults[key]), placeholders(value)
      for token, occurrences in pairs(expected) do
        equal(actual[token], occurrences, path .. ": " .. key .. " marker ^" .. token)
      end
      for token, occurrences in pairs(actual) do
        equal(expected[token], occurrences, path .. ": " .. key .. " marker ^" .. token)
      end
    end
    for key in pairs(defaults) do
      assert(translations[key], "Missing key in " .. path .. ": " .. key)
    end
  end
end)

-- Runner

local failed, printedSection = 0, nil
for _, item in ipairs(tests) do
  if item.section ~= printedSection then
    print("\n" .. item.section)
    printedSection = item.section
  end
  local ok, message = xpcall(function()
    reset()
    item.run()
    assert(not env.unexpected, env.unexpected)
    assert(#env.responses == 0, "Unconsumed HTTP response fixtures: " .. #env.responses)
    for _, call in ipairs(env.calls) do
      if call.url:sub(1, #apiOrigin + 1) == apiOrigin .. "/" then
        if call.method == "GET" then
          equal(call.body, "")
          equal(header(call, "Content-Type"), nil)
        else
          payload(call)
        end
      end
    end
  end, debug.traceback)
  _G.io = realIO
  if ok then
    print("PASS " .. item.name)
  else
    failed = failed + 1
    print("FAIL " .. item.name .. ": " .. tostring(message))
  end
end
print(string.format("\n%d tests, %d failures", #tests, failed))
if failed > 0 then
  os.exit(1)
end
