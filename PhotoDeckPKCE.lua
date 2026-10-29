local LrFileUtils = import("LrFileUtils")
local LrPathUtils = import("LrPathUtils")
local LrStringUtils = import("LrStringUtils")
local LrTasks = import("LrTasks")
local JSON = require("PhotoDeckJSON")

local PKCE = {}

function PKCE.generate()
  local path = LrFileUtils.chooseUniqueFileName(LrPathUtils.child(LrPathUtils.getStandardFilePath("temp"), "photodeck-pkce.json"))
  local created = false
  local ok, result = LrTasks.pcall(function()
    local command
    if WIN_ENV then
      -- EncodedCommand avoids cmd.exe interpreting characters in file paths.
      local function literal(value)
        return "'" .. value:gsub("'", "''") .. "'"
      end
      local script = "& " .. literal(LrPathUtils.child(_PLUGIN.path, "oauth-pkce.ps1")) .. " -OutputPath " .. literal(path)
      -- PowerShell expects UTF-16LE, including for non-ASCII installation paths.
      local utf16 = {}
      local index = 1
      while index <= #script do
        local first = script:byte(index)
        local code, size = first, 1
        if first >= 240 then
          code, size = first % 8, 4
        elseif first >= 224 then
          code, size = first % 16, 3
        elseif first >= 192 then
          code, size = first % 32, 2
        end
        for offset = 1, size - 1 do
          code = code * 64 + script:byte(index + offset) % 64
        end
        if code > 65535 then
          code = code - 65536
          local high = 55296 + math.floor(code / 1024)
          utf16[#utf16 + 1] = string.char(high % 256, math.floor(high / 256))
          code = 56320 + code % 1024
        end
        utf16[#utf16 + 1] = string.char(code % 256, math.floor(code / 256))
        index = index + size
      end
      command = "powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -EncodedCommand " .. LrStringUtils.encodeBase64(table.concat(utf16))
    else
      local function quote(value)
        return "'" .. value:gsub("'", "'\\''") .. "'"
      end
      command = "/bin/sh " .. quote(LrPathUtils.child(_PLUGIN.path, "oauth-pkce.sh")) .. " " .. quote(path)
    end
    if LrTasks.execute(command) ~= 0 then
      return nil
    end
    created = true
    local file = io.open(path, "rb")
    if not file then
      return nil
    end
    local text = file:read(4096)
    file:close()
    local value = JSON.decode(text or "")
    for _, key in ipairs({ "verifier", "state", "challenge" }) do
      if type(value[key]) ~= "string" or #value[key] ~= 43 or value[key]:find("[^A-Za-z0-9_-]") then
        return nil
      end
    end
    return value
  end)
  if created then
    local cleaned, removed = LrTasks.pcall(function()
      return LrFileUtils.delete(path)
    end)
    if not cleaned or not removed then
      return nil, LOC("$$$/PhotoDeck/OAuth/CryptoFailed=Could not generate secure authorization data")
    end
  end
  if ok and result then
    return result
  end
  return nil, LOC("$$$/PhotoDeck/OAuth/CryptoFailed=Could not generate secure authorization data")
end

return PKCE
