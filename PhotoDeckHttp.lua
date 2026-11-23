local LrDate = import("LrDate")
local LrFileUtils = import("LrFileUtils")
local LrHttp = import("LrHttp")
local LrTasks = import("LrTasks")
local JSON = require("PhotoDeckJSON")

local logger = import("LrLogger")("PhotoDeckPublishLightroomPlugin")
logger:enable("logfile")
local log_trace = logger:quick("trace")

local Http = {}
local months = { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" }

function Http.encode(value)
  if type(value) == "boolean" then
    value = value and "1" or "0"
  end
  return (tostring(value):gsub("([^A-Za-z0-9_.~-])", function(char)
    return string.format("%%%02X", char:byte())
  end))
end

local function formValue(value)
  if type(value) == "boolean" then
    return value and "1" or "0"
  end
  return tostring(value)
end

local function queryFields(params)
  if params and params[1] then
    return params
  end
  local result, keys = {}, {}
  for key in pairs(params or {}) do
    keys[#keys + 1] = key
  end
  table.sort(keys)
  for _, key in ipairs(keys) do
    local value = params[key]
    if type(value) == "table" then
      for _, item in ipairs(value) do
        result[#result + 1] = { name = key, value = item }
      end
    else
      result[#result + 1] = { name = key, value = value }
    end
  end
  return result
end

function Http.formEncode(params)
  local result = {}
  for _, field in ipairs(queryFields(params)) do
    result[#result + 1] = Http.encode(field.name) .. "=" .. Http.encode(field.value)
  end
  return table.concat(result, "&")
end

local function parseDate(value)
  local day, month, year, hour, minute, second = value:match("%a+, (%d+) (%a+) (%d+) (%d+):(%d+):(%d+) GMT")
  for number, name in ipairs(months) do
    if name == month then
      return LrDate.timeFromComponents(tonumber(year), number, tonumber(day), tonumber(hour), tonumber(minute), tonumber(second), 0)
    end
  end
end

local function responseInfo(headers)
  headers = headers or {}
  local info = { status = tonumber(headers.status) or 0, headers = {}, links = {} }
  for _, header in ipairs(headers) do
    local name = header.field:lower()
    local values = info.headers[name] or {}
    values[#values + 1] = header.value
    info.headers[name] = values
    if name == "status" then
      info.status = tonumber(header.value:match("^(%d%d%d)")) or info.status
    elseif name == "link" then
      -- A URL or quoted parameter may contain commas; split at <...>, not ','.
      for url, parameters in header.value:gmatch("<([^>]+)>([^<]*)") do
        local relations = parameters:match('[;,%s]rel%s*=%s*"([^"]+)"') or parameters:match("[;,%s]rel%s*=%s*([^;, %s]+)")
        for relation in (relations or ""):gmatch("%S+") do
          info.links[relation] = url
        end
      end
    end
  end
  info.request_id = info.headers["x-request-id"] and info.headers["x-request-id"][1]
  info.network_error = headers.error
  if headers.error and info.status >= 200 and info.status < 300 then
    -- A connection can fail after the HTTP status/headers have been received.
    info.status = 0
  end
  return info
end

Http.responseInfo = responseInfo

local function header(info, name)
  return info.headers[name] and info.headers[name][1]
end

-- Read the named items in a structured-field list. Quoted values can contain
-- commas and semicolons, so neither headers nor parameters can be split blindly.
local function ratePolicies(info, name)
  local text = table.concat(info.headers[name] or {}, ",")
  local position, policies = 1, {}
  local function consume(pattern)
    local value = text:sub(position):match("^" .. pattern)
    if value then position = position + #value end
    return value
  end
  local function stringValue()
    if not consume('"') then return nil end
    local result = {}
    while position <= #text do
      local char = text:sub(position, position)
      position = position + 1
      if char == '"' then return table.concat(result) end
      if char == "\\" then
        char = text:sub(position, position)
        if char ~= '"' and char ~= "\\" then return nil end
        position = position + 1
      elseif char:byte() < 32 or char:byte() > 126 then
        return nil
      end
      result[#result + 1] = char
    end
  end
  consume("[ \t]*")
  while position <= #text do
    local policy = stringValue()
    if not policy then return {} end
    local parameters = {}
    while consume(";") do
      consume(" *")
      local key = consume("[a-z*][a-z0-9_.*-]*")
      if not key then return {} end
      local value = true
      if consume("=") then
        if text:sub(position, position) == '"' then
          value = stringValue()
        else
          local raw = consume("[^;, \t]+")
          if not raw then return {} end
          local valid = raw:match("^[%a*][%w!#$%%&'*+.^_`|~:/-]*$")
            or raw:match("^:[%w+/=]*:$") or raw:match("^%?[01]$")
            or (#raw:gsub("^-", "") <= 15 and raw:match("^%-?%d+$"))
            or raw:match("^%-?%d+%.%d%d?%d?$") or raw:match("^@%-?%d+$")
          if not valid then return {} end
          -- Only integer-typed values may be used for pacing parameters.
          value = #raw <= 15 and raw:match("^%d+$") and tonumber(raw) or false
        end
        if value == nil then return {} end
      end
      parameters[key] = value
    end
    for _, key in ipairs({ "q", "w", "r", "t" }) do
      if parameters[key] ~= nil and (type(parameters[key]) ~= "number" or (key == "w" and parameters[key] == 0)) then
        return {}
      end
    end
    policies[policy] = parameters
    consume("[ \t]*")
    if position > #text then break end
    if not consume(",") then return {} end
    consume("[ \t]*")
    if position > #text then return {} end
  end
  return policies
end

local function normalize(value)
  if type(value) == "table" and value ~= JSON.null then
    for key, item in pairs(value) do
      if item == JSON.null and not JSON.isArray(value) then
        value[key] = nil
      else
        normalize(item)
      end
    end
  end
  return value
end

local function decode(body, info)
  if info.status == 204 then
    return nil
  end
  local contentType = (header(info, "content-type") or ""):lower():match("^%s*([^;%s]+)")
  if contentType ~= "application/json" and not (contentType and contentType:match("^application/[%w.+-]+%+json$")) then
    return nil, LOC("$$$/PhotoDeck/API/InvalidResponse=Invalid response from PhotoDeck")
  end
  local ok, value = pcall(JSON.decode, body or "")
  if not ok or (not JSON.isObject(value) and not JSON.isArray(value)) then
    return nil, LOC("$$$/PhotoDeck/API/InvalidResponse=Invalid response from PhotoDeck")
  end
  return normalize(value)
end

local function quoted(value)
  return (tostring(value):gsub("[\r\n]", ""):gsub('[\\"]', "\\%0"))
end

-- Multipart storage transfers using methods other than the SDK's native POST.
local function multipart(params)
  local boundary = string.format("photodeck-%08x-%08x", math.random(99999999), math.random(99999999))
  local parts = {}
  for _, field in ipairs(params) do
    local disposition = "--" .. boundary .. '\r\nContent-Disposition: form-data; name="' .. quoted(field.name) .. '"'
    if field.filePath then
      disposition = disposition .. '; filename="' .. quoted(field.fileName) .. '"'
    end
    if field.contentType then
      disposition = disposition .. "\r\nContent-Type: " .. field.contentType:gsub("[\r\n]", "")
    end
    parts[#parts + 1] = disposition .. "\r\n\r\n"
    parts[#parts + 1] = field.filePath and { filePath = field.filePath } or formValue(field.value)
    parts[#parts + 1] = "\r\n"
  end
  parts[#parts + 1] = "--" .. boundary .. "--\r\n"
  return parts, "multipart/form-data; boundary=" .. boundary
end

local function requestFailure(failure, fileError)
  -- SDK exceptions can contain signed URLs and local paths. Keep only a fixed
  -- category; never pass the original exception into logs or display messages.
  local kind = tostring(failure):find("yield", 1, true) and "yieldNotAllowed"
    or tostring(failure):find("bad argument", 1, true) and "invalidArgument"
    or "luaException"
  return nil, { error = { errorCode = fileError and "fileReadError" or "requestFailed", exceptionKind = kind } }
end

local function send(method, url, headers, parts)
  local files, fileError, index, size = {}, false, 1, 0
  local function closeFiles()
    for _, file in pairs(files) do
      file:close()
    end
  end
  if type(parts) == "table" then
    local hasFile = false
    for _, part in ipairs(parts) do
      if type(part) == "table" then
        hasFile = true
        local attributes = LrFileUtils.fileAttributes(part.filePath)
        if not attributes or not attributes.fileSize then
          return nil, { error = { errorCode = "fileReadError" } }
        end
        size = size + attributes.fileSize
      else
        size = size + #part
      end
    end
    headers[#headers + 1] = { field = "Content-Length", value = tostring(size) }
    if not hasFile then
      -- Only requests containing file parts need a streaming body.
      parts = table.concat(parts)
    else
      -- Prepare handles in the task, not in Lightroom's native byte-getter
      -- callback. That callback must only supply bytes from already-open files.
      for position, part in ipairs(parts) do
        if type(part) == "table" then
          local file = io.open(part.filePath, "rb")
          if not file then
            closeFiles()
            return nil, { error = { errorCode = "fileReadError" } }
          end
          files[position] = file
        end
      end
    end
  end
  local function readChunk()
    while index <= #parts do
      local part = parts[index]
      if type(part) == "string" then
        index = index + 1
        if #part > 0 then
          return part
        end
      else
        local chunk, readError = files[index]:read(1048576)
        if readError then
          fileError = true
          error("File read error")
        end
        if chunk then
          return chunk
        end
        index = index + 1
      end
    end
  end
  local ok, body, responseHeaders = LrTasks.pcall(function()
    if method == "GET" then
      return LrHttp.get(url, headers)
    end
    return LrHttp.post(url, type(parts) == "table" and readChunk or parts, headers, method, 60, type(parts) == "table" and size or nil)
  end)
  closeFiles()
  if not ok then
    return requestFailure(body, fileError)
  end
  return body, responseHeaders
end

local function sendMultipartPost(url, params, headers)
  -- Let Lightroom generate the boundary and stream the file for native POSTs.
  local ok, body, responseHeaders = LrTasks.pcall(function()
    return LrHttp.postMultipart(url, params, headers, 60)
  end)
  if not ok then
    return requestFailure(body)
  end
  return body, responseHeaders
end

local networkErrorCodes = {
  cancelled = true,
  badURL = true,
  timedOut = true,
  cannotFindHost = true,
  cannotConnectToHost = true,
  resourceUnavailable = true,
  networkConnectionLost = true,
  redirectError = true,
  badServerResponse = true,
  authenticationError = true,
  securityError = true,
  serverCertificateHasBadDate = true,
  serverCertificateHasUnknownRoot = true,
  fileReadError = true,
  requestFailed = true,
}

function Http.networkErrorCode(info)
  local network = info.network_error or {}
  return networkErrorCodes[network.errorCode] and network.errorCode or (info.network_error and "unknown" or "none")
end

local function logTransfer(media, info)
  -- Only machine codes and the non-secret location identifier, never URLs,
  -- response bodies, form parameters, cookie values or localized SDK messages.
  local location = type(media.upload_location) == "string" and media.upload_location:match("^[%w_-]+$") or "unknown"
  local network = info.network_error or {}
  local code = Http.networkErrorCode(info)
  local native = tonumber(network.nativeCode)
  local kind = network.exceptionKind
  if kind ~= "yieldNotAllowed" and kind ~= "invalidArgument" and kind ~= "luaException" then
    kind = "none"
  end
  log_trace(
    string.format(" <- storage [%s] HTTP %d; network=%s; native=%s; exception=%s", location, info.status, code, native and tostring(native) or "none", kind)
  )
end

function Http.new(state, baseURL)
  local client = {}
  local nextRequest, blockedUntil, interval, lastRequest = 0, 0, 1, nil
  local rateLimit = { aggressiveness = 10 }
  local origin = assert(baseURL:match("^(https://[^/]+)$"))

  function client.delayNext(seconds)
    nextRequest = math.max(nextRequest, LrDate.currentTime() + seconds)
  end

  function client.target(url)
    if type(url) ~= "string" or url:find("[\r\n#]") then
      return nil
    end
    if url:sub(1, #origin + 1) == origin .. "/" then
      url = url:sub(#origin + 1)
    end
    if url:match("^%a[%w+.-]*:") or url:sub(1, 2) == "//" or url:find("\\", 1, true) then
      return nil
    end
    if url:sub(1, 1) ~= "/" then
      url = "/" .. url
    end
    local path = url:match("^[^?]+")
    if not path or path:sub(-5) ~= ".json" then
      return nil
    end
    return url
  end

  local function throttle()
    local function readyAt()
      return math.max(nextRequest, blockedUntil, lastRequest and lastRequest + interval or 0)
    end
    while true do
      local wait = readyAt() - LrDate.currentTime()
      if wait <= 0 then
        break
      end
      LrTasks.sleep(wait)
    end
    lastRequest = LrDate.currentTime()
  end

  local function updateLimits(info, retries)
    local policy = ratePolicies(info, "ratelimit-policy").requests
    local limit = ratePolicies(info, "ratelimit").requests
    if policy and policy.q then
      rateLimit.capacity, rateLimit.window = policy.q, policy.w
    end
    local retry = header(info, "retry-after")
    if retry then
      local date = parseDate(retry)
      retry = tonumber(retry:match("^%s*(%d+)%s*$")) or (date and math.max(0, date - LrDate.currentTime()))
    end
    if limit and limit.r then
      local capacity, window = rateLimit.capacity, rateLimit.window
      if limit.r == 0 then
        -- Retry-After takes precedence over this response's effective window.
        interval = retry or limit.t or window or interval
      elseif capacity and window then
        -- Use w, not the effective window t, for adaptive background pacing.
        interval = capacity == 0 and window or window * (math.max(0, (capacity - limit.r) / capacity)) ^ rateLimit.aggressiveness
      end
      client.delayNext(interval)
      log_trace(string.format(" Rate limiting: requests remaining=%g; delay=%.3fs", limit.r, interval))
    end
    if retry then
      blockedUntil = math.max(blockedUntil, LrDate.currentTime() + retry)
    elseif info.status == 429 or info.status == 0 then
      -- Pause all API work, even when a caller handles the error or an
      -- ambiguous mutation cannot itself be retried safely.
      client.delayNext(2 ^ (retries + 1) + math.random())
    end
  end

  function client.request(method, target, params, onerror)
    target = client.target(target)
    if not target then
      return nil, LOC("$$$/PhotoDeck/API/UntrustedURL=Invalid or untrusted PhotoDeck API URL")
    end
    if method == "GET" and params and next(params) then
      target = target .. (target:find("?", 1, true) and "&" or "?") .. Http.formEncode(params)
    end
    local parts, contentType = "", nil
    if method ~= "GET" then
      parts, contentType = JSON.encode(params or {}), "application/json"
    end
    local redirects, retries, authRetries = 0, 0, 0
    -- Pin the connection for this request, even if another service is selected
    -- while Lightroom yields during throttling or HTTP operations.
    local auth = state
    if type(state) == "function" then
      auth = state()
    end
    local rejected
    while true do
      local token, authError
      if auth then
        token, authError = auth.getAccessToken(rejected)
      end
      if not token then
        log_trace(auth and "OAuth: API request blocked; access token unavailable" or "OAuth: API request blocked; no connection context")
        return nil, authError or LOC("$$$/PhotoDeck/OAuth/ReconnectRequired=Please reconnect to PhotoDeck in the publish service settings")
      end
      -- Token acquisition can yield while another request refreshes. Reserve
      -- the API slot afterward so newer cooldowns and concurrent sends apply.
      throttle()
      local headers = {
        { field = "Authorization", value = "Bearer " .. token },
        { field = "Accept", value = "application/json" },
        -- An explicit header also isolates us from Lightroom's shared cookie jar.
        { field = "Cookie", value = "" },
      }
      if contentType then
        headers[#headers + 1] = { field = "Content-Type", value = contentType }
      end
      log_trace(string.format(" -> %s %s", method, target:match("^[^?]+")))
      local body, responseHeaders = send(method, origin .. target, headers, parts)
      local info = responseInfo(responseHeaders)
      updateLimits(info, retries)
      log_trace(string.format(" <- %d [%s]", info.status, info.request_id or "No request ID"))
      if info.status == 307 then
        redirects = redirects + 1
        target = client.target(header(info, "location"))
        if not target or redirects > 3 then
          return nil, LOC("$$$/PhotoDeck/API/RedirectFailed=Invalid or excessive PhotoDeck redirect"), info
        end
      else
        local result, decodeError = decode(body, info)
        info.body = result
        if info.status >= 200 and info.status < 300 then
          if decodeError then
            return nil, decodeError, info
          end
          return result, nil, info
        end
        local message = type(result) == "table" and result.error
        if type(message) ~= "string" or message == "" then
          message = info.network_error and info.network_error.errorCode == "fileReadError" and LOC("$$$/PhotoDeck/API/FileReadError=Error reading file")
            or info.status == 0 and LOC("$$$/PhotoDeck/API/NoResponse=No response from network")
            or LOC("$$$/PhotoDeck/API/HTTPError=HTTP error ^1", info.status)
        end
        if result and result.stop_with_error == true then
          return nil, message, info
        end
        if onerror and onerror[tostring(info.status)] then
          local value, errorMessage = onerror[tostring(info.status)]()
          return value, errorMessage, info
        end
        if info.status == 401 and authRetries < 1 then
          authRetries, rejected = authRetries + 1, token
        elseif info.status == 401 then
          auth.reject(token)
          return nil, LOC("$$$/PhotoDeck/OAuth/ReconnectRequired=Please reconnect to PhotoDeck in the publish service settings"), info
        elseif info.status == 429 and retries < 3 then
          retries = retries + 1
        elseif method == "GET" and (info.status == 0 or info.status >= 500) and retries < 3 then
          retries = retries + 1
          if info.status ~= 0 then
            client.delayNext(2 ^ retries + math.random())
          end
        else
          return nil, message, info
        end
      end
    end
  end

  function client.transfer(media, contentPath, fileSize)
    local attributes = LrFileUtils.fileAttributes(contentPath)
    if not attributes or attributes.fileSize ~= fileSize then
      return nil, LOC("$$$/PhotoDeck/API/FileReadError=Error reading file"), false
    end
    local url = media.upload_url
    local storageOrigin = type(url) == "string" and url:match("^(https://[^/]+)")
    if not storageOrigin or storageOrigin:find("@", 1, true) then
      return nil, LOC("$$$/PhotoDeck/API/Media/UploadURLMissing=Upload URL missing"), false
    end
    for attempt = 1, 4 do
      -- Never forward bearer credentials or API headers to storage.
      -- No Authorization header at all: storage authorizes through the opaque
      -- signed URL/form. Even an empty header can interfere with that scheme.
      local headers = { { field = "Cookie", value = "" } }
      local parts, nativeMultipart
      if media.upload_file_param then
        local params = {}
        for key, value in pairs(media.upload_params or {}) do
          params[#params + 1] = { name = key, value = formValue(value) }
        end
        params[#params + 1] = { name = media.upload_file_param, filePath = contentPath, fileName = media.file_name, contentType = media.mime_type }
        if media.upload_method == "POST" then
          nativeMultipart = params
        else
          local contentType
          parts, contentType = multipart(params)
          headers[#headers + 1] = { field = "Content-Type", value = contentType }
        end
      else
        parts = { { filePath = contentPath } }
        headers[#headers + 1] = { field = "Content-Type", value = media.mime_type }
      end
      log_trace(string.format(" -> storage %s (%s, %d file bytes)", media.upload_method, media.upload_file_param and "multipart" or "raw", fileSize))
      local responseHeaders
      if nativeMultipart then
        local ignored
        ignored, responseHeaders = sendMultipartPost(url, nativeMultipart, headers)
      else
        local ignored
        ignored, responseHeaders = send(media.upload_method, url, headers, parts)
      end
      local info = responseInfo(responseHeaders)
      logTransfer(media, info)
      if info.network_error and info.network_error.errorCode == "fileReadError" then
        return nil, LOC("$$$/PhotoDeck/API/FileReadError=Error reading file"), false
      elseif info.status >= 200 and info.status < 300 then
        return true
      elseif info.status == 307 and attempt < 4 then
        local location = header(info, "location") or ""
        if location:sub(1, 1) == "/" and location:sub(1, 2) ~= "//" then
          location = storageOrigin .. location
        end
        if location:sub(1, #storageOrigin + 1) ~= storageOrigin .. "/" or location:find("[\r\n#\\]") then
          break
        end
        url = location
      else
        local message = info.status == 0 and LOC("$$$/PhotoDeck/API/NoResponse=No response from network")
          or LOC("$$$/PhotoDeck/API/HTTPError=HTTP error ^1", info.status)
        -- Storage-specific bodies are intentionally neither JSON-decoded nor logged.
        return nil, message, info.status == 0 or info.status == 408 or info.status == 429 or info.status >= 500
      end
    end
    return nil, LOC("$$$/PhotoDeck/API/RedirectFailed=Invalid or excessive PhotoDeck redirect"), false
  end

  return client
end

return Http
