local LrDate = import("LrDate")
local LrDigest = import("LrDigest")
local LrDialogs = import("LrDialogs")
local LrFileUtils = import("LrFileUtils")
local LrHttp = import("LrHttp")
local LrStringUtils = import("LrStringUtils")
local LrTasks = import("LrTasks")
local LrView = import("LrView")
local JSON = require("PhotoDeckJSON")

local logger = import("LrLogger")("PhotoDeckPublishLightroomPlugin")
logger:enable("logfile")
local log_trace = logger:quick("trace")

local Http = {}
local months = { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" }
local weekdays = { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" }

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

local function queryEncode(params)
  local result = {}
  for _, field in ipairs(queryFields(params)) do
    result[#result + 1] = Http.encode(field.name) .. "=" .. Http.encode(field.value)
  end
  return table.concat(result, "&")
end

local function timestamp()
  -- LrDate's user-format function localizes day/month names, even in GMT.
  local year, month, day, hour, minute, second, weekday = LrDate.timestampToComponents(LrDate.currentTime(), 0)
  return string.format("%s, %02d %s %04d %02d:%02d:%02d GMT", weekdays[weekday], day, months[month], year, hour, minute, math.floor(second))
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

local function header(info, name)
  return info.headers[name] and info.headers[name][1]
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

local function logTransfer(media, info)
  -- Only machine codes and the non-secret location identifier, never URLs,
  -- response bodies, form parameters, cookie values or localized SDK messages.
  local location = type(media.upload_location) == "string" and media.upload_location:match("^[%w_-]+$") or "unknown"
  local network = info.network_error or {}
  local code = networkErrorCodes[network.errorCode] and network.errorCode or (info.network_error and "unknown" or "none")
  local native = tonumber(network.nativeCode)
  local kind = network.exceptionKind
  if kind ~= "yieldNotAllowed" and kind ~= "invalidArgument" and kind ~= "luaException" then
    kind = "none"
  end
  log_trace(
    string.format(" <- storage [%s] HTTP %d; network=%s; native=%s; exception=%s", location, info.status, code, native and tostring(native) or "none", kind)
  )
end

local function promptOTP(state)
  state.otp = nil
  local factory = LrView.osFactory()
  local result = LrDialogs.presentModalDialog({
    title = LOC("$$$/PhotoDeck/OTPDialog/Title=PhotoDeck Two-factor authentication"),
    contents = factory:column({
      spacing = factory:dialog_spacing(),
      bind_to_object = state,
      factory:row({
        factory:static_text({ title = LOC("$$$/PhotoDeck/OTPDialog/Code=Authentication code:"), alignment = "right" }),
        factory:edit_field({ value = LrView.bind("otp"), immediate = false, width_in_chars = 6 }),
      }),
    }),
  })
  return result == "ok" and state.otp and state.otp ~= ""
end

function Http.new(state, baseURL)
  local client = {}
  local cookies = {}
  local nextRequest, blockedUntil, interval, lastRequest = 0, 0, 1, nil
  local rateLimit = { aggressiveness = 10, capacity = 60, remaining = 60, window = 60, reset = 60 }
  local origin = assert(baseURL:match("^(https://[^/]+)$"))
  local hostname = origin:match("^https://([^:]+)"):lower()

  function client.reset()
    cookies = {}
  end

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

  local function retainCookies(info, path)
    for _, raw in ipairs(info.headers["set-cookie"] or {}) do
      local name, value = raw:match("^%s*([^=;%s]+)=([^;]*)")
      if name then
        local cookie = { name = name, value = value, path = path:match("^(.+)/") or "/", domain = hostname }
        for attribute in raw:gmatch(";([^;]+)") do
          local key, item = attribute:match("^%s*([^=%s]+)%s*=?%s*(.-)%s*$")
          key = key and key:lower()
          if key == "path" then
            if item:sub(1, 1) == "/" then
              cookie.path = item
            end
          elseif key == "domain" then
            cookie.domain = item:lower():gsub("^%.", "")
          elseif key == "max-age" and tonumber(item) then
            cookie.expires = LrDate.currentTime() + tonumber(item)
          elseif key == "expires" and not cookie.expires then
            cookie.expires = parseDate(item)
          end
        end
        if cookie.domain == hostname or hostname:sub(-#cookie.domain - 1) == "." .. cookie.domain then
          local key = cookie.domain .. "\n" .. cookie.path .. "\n" .. name
          cookies[key] = cookie
        end
      end
    end
  end

  local function cookieHeader(path)
    local values = {}
    for key, cookie in pairs(cookies) do
      if cookie.expires and cookie.expires <= LrDate.currentTime() then
        cookies[key] = nil
      elseif
        path:sub(1, #cookie.path) == cookie.path
        and (path == cookie.path or cookie.path:sub(-1) == "/" or path:sub(#cookie.path + 1, #cookie.path + 1) == "/")
      then
        values[#values + 1] = cookie.name .. "=" .. cookie.value
      end
    end
    table.sort(values)
    return table.concat(values, "; ")
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

  local function updateLimits(info)
    local limit = header(info, "ratelimit-limit")
    local remaining = tonumber(header(info, "ratelimit-remaining"))
    local reset = tonumber(header(info, "ratelimit-reset"))
    if reset and reset >= 0 then
      rateLimit.reset, rateLimit.window = reset, reset
    end
    if limit then
      local capacity = tonumber(limit:match("^%s*([^,;]+)"))
      if capacity and capacity >= 0 then
        rateLimit.capacity = capacity
        local maxWindow = 0
        for policy in limit:gmatch("[^,]+") do
          local quota, parameters = policy:match("^%s*(%d+)%s*(;.*)$")
          quota = tonumber(quota)
          if quota and quota <= capacity then
            for window in parameters:gmatch(";%s*w%s*=%s*(%d+)") do
              maxWindow = math.max(maxWindow, tonumber(window))
            end
          end
        end
        if maxWindow > 0 then
          rateLimit.window = maxWindow
        end
      end
    end
    if remaining then
      rateLimit.remaining = remaining
      if remaining <= 0 then
        interval = rateLimit.reset
        blockedUntil = math.max(blockedUntil, LrDate.currentTime() + interval)
      elseif rateLimit.capacity <= 0 then
        interval = rateLimit.window
      elseif remaining < rateLimit.capacity then
        -- Smooth traffic increasingly as the shared quota fills. Use the full
        -- policy window, not just the seconds left until the next reset.
        interval = ((rateLimit.capacity - remaining) / rateLimit.capacity) ^ rateLimit.aggressiveness * rateLimit.window
      else
        interval = 0
      end
      client.delayNext(interval)
      log_trace(
        string.format(
          " Rate limiting: remaining=%g/%g; window=%g; reset=%g; delay=%.3fs",
          remaining,
          rateLimit.capacity,
          rateLimit.window,
          rateLimit.reset,
          interval
        )
      )
    end
    local retry = header(info, "retry-after")
    retry = retry and (tonumber(retry) or ((parseDate(retry) or LrDate.currentTime()) - LrDate.currentTime()))
    if info.status == 429 then
      blockedUntil = math.max(blockedUntil, LrDate.currentTime() + (retry or rateLimit.reset))
    elseif retry then
      blockedUntil = math.max(blockedUntil, LrDate.currentTime() + retry)
    elseif info.status == 0 then
      -- Native networking can hide throttling responses. Leave a reset window
      -- of silence before issuing further API requests after a network failure.
      blockedUntil = math.max(blockedUntil, LrDate.currentTime() + rateLimit.reset)
    end
  end

  function client.request(method, target, params, onerror)
    target = client.target(target)
    if not target then
      return nil, LOC("$$$/PhotoDeck/API/UntrustedURL=Invalid or untrusted PhotoDeck API URL")
    end
    if method == "GET" and params and next(params) then
      target = target .. (target:find("?", 1, true) and "&" or "?") .. queryEncode(params)
    end
    local parts, contentType = "", nil
    if method ~= "GET" then
      parts, contentType = JSON.encode(params or {}), "application/json"
    end
    local redirects, retries, authRetries = 0, 0, 0
    while true do
      local path, query = target:match("^([^?]+)%??(.*)$")
      local needsUser = path ~= "/ping.json" and path ~= "/logout.json"
      local wasLoggedIn = state.loggedin
      throttle()
      local time = timestamp()
      local signature = LrDigest.SHA1.digest(string.format("%s\n%s\n%s\n%s\n%s\n", method, path, query, state.secret, time)):lower()
      local headers = {
        { field = "X-PhotoDeck-Timestamp", value = time },
        { field = "X-PhotoDeck-Authorization", value = state.key .. ":" .. signature },
        { field = "Accept", value = "application/json" },
        -- An explicit header also isolates us from Lightroom's shared cookie jar.
        { field = "Cookie", value = cookieHeader(path) },
      }
      if needsUser and not state.loggedin and state.username and state.username ~= "" and state.password and state.password ~= "" then
        local password = state.password
        if state.otpEnabled and state.otp then
          password = password .. "+" .. state.otp
          state.otp = nil
        end
        headers[#headers + 1] = { field = "Authorization", value = "Basic " .. LrStringUtils.encodeBase64(state.username .. ":" .. password) }
      end
      if contentType then
        headers[#headers + 1] = { field = "Content-Type", value = contentType }
      end
      log_trace(string.format(" -> %s %s", method, path))
      local body, responseHeaders = send(method, origin .. target, headers, parts)
      local info = responseInfo(responseHeaders)
      retainCookies(info, path)
      updateLimits(info)
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
          if needsUser then
            state.loggedin = true
          end
          return result, nil, info
        end
        local message = type(result) == "table" and result.error
        if type(message) ~= "string" or message == "" then
          message = info.network_error and info.network_error.errorCode == "fileReadError" and LOC("$$$/PhotoDeck/API/FileReadError=Error reading file")
            or info.status == 0 and LOC("$$$/PhotoDeck/API/NoResponse=No response from network")
            or LOC("$$$/PhotoDeck/API/HTTPError=HTTP error ^1", info.status)
        end
        if info.status == 401 then
          state.loggedin = false
        end
        if result and result.stop_with_error == true then
          return nil, message, info
        end
        if onerror and onerror[tostring(info.status)] then
          local value, errorMessage = onerror[tostring(info.status)]()
          return value, errorMessage, info
        end
        if info.status == 401 and authRetries < 3 then
          authRetries = authRetries + 1
          if result and result.auth_method == "basic:password+otp" then
            state.otpEnabled = true
            if not promptOTP(state) then
              return nil, message, info
            end
          elseif wasLoggedIn then
            state.otpEnabled, state.otp = false, nil
          else
            state.otpEnabled, state.otp = false, nil
            return nil, message, info
          end
        elseif info.status == 429 and retries < 3 then
          retries = retries + 1
        elseif method == "GET" and (info.status == 0 or info.status >= 500) and retries < 3 then
          retries = retries + 1
          client.delayNext(2 ^ retries + math.random())
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
      -- Never forward API headers, Basic credentials or session cookies to storage.
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
