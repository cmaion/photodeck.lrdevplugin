local LrDate = import("LrDate")
local LrHttp = import("LrHttp")
local LrPasswords = import("LrPasswords")
local LrStringUtils = import("LrStringUtils")
local LrTasks = import("LrTasks")
local JSON = require("PhotoDeckJSON")
local Http = require("PhotoDeckHttp")
local PKCE = require("PhotoDeckPKCE")

local logger = import("LrLogger")("PhotoDeckPublishLightroomPlugin")
logger:enable("logfile")
local log_trace = logger:quick("trace")

local OAuth = { redirectURI = "lightroom://" .. _PLUGIN.id .. "/oauth/callback" }
local connections, pending = {}, {}
-- Limit abandoned browser interactions; this is not a code/token lifetime.
local browserTimeout = 300

local oauthErrors = {
  invalid_request = true,
  invalid_client = true,
  invalid_grant = true,
  unauthorized_client = true,
  unsupported_grant_type = true,
  unsupported_response_type = true,
  invalid_scope = true,
  invalid_target = true,
  access_denied = true,
  server_error = true,
  temporarily_unavailable = true,
}

local function errorCode(value)
  return type(value) == "string" and oauthErrors[value] and value or "unknown"
end

local function logException(stage, failure)
  -- SDK exceptions can contain request bodies or URLs. Keep only the category
  -- and a source location from our Lua files, never the exception itself.
  local text = type(failure) == "string" and failure or ""
  local category = text:find("yield", 1, true) and "yieldNotAllowed"
    or text:find("bad argument", 1, true) and "invalidArgument"
    or "luaException"
  local source = text:match("(PhotoDeck[%w_]*%.lua:%d+):") or "unknown"
  log_trace(string.format("OAuth: %s failed; exception=%s; source=%s", stage, category, source))
end

local function reconnectMessage()
  return LOC("$$$/PhotoDeck/OAuth/ReconnectRequired=Please reconnect to PhotoDeck in the publish service settings")
end

local function storageMessage()
  return LOC("$$$/PhotoDeck/OAuth/StorageFailed=Could not access secure credential storage")
end

local function authorizationFailure(callback, data)
  local detail = errorCode(callback.error)
  local description = callback.error_description
  if type(description) == "string" and description ~= "" then
    -- This server-supplied text is for the dialog only, never the log. Redact
    -- private attempt data if the server happens to echo it in its explanation.
    for _, secret in ipairs({ data.state, data.verifier }) do
      description = description:gsub(secret:gsub("(%W)", "%%%1"), "[redacted]")
    end
    description = LrStringUtils.truncate(description:gsub("%c", " "), 512)
    detail = detail .. ": " .. description
  end
  return LOC("$$$/PhotoDeck/OAuth/AuthorizationFailed=PhotoDeck authorization failed: ^1", detail)
end

local function tokenFailure(status, value)
  local detail = status == 0 and LOC("$$$/PhotoDeck/API/NoResponse=No response from network")
    or status == 200 and LOC("$$$/PhotoDeck/API/InvalidResponse=Invalid response from PhotoDeck")
    or LOC("$$$/PhotoDeck/API/HTTPError=HTTP error ^1", status)
  if value and value.error then
    local code = errorCode(value.error)
    if code ~= "unknown" then
      detail = detail .. "; " .. code
    end
  end
  return LOC("$$$/PhotoDeck/OAuth/TokenExchangeFailed=PhotoDeck token exchange failed (^1). Please reconnect.", detail)
end

-- OAuth exchanges are deliberately never retried or redirected. A lost response
-- can mean that a single-use code/refresh token has already been consumed.
local function post(issuer, endpoint, params)
  local operation = endpoint == "/oauth/revoke" and "revoke" or params.grant_type
  log_trace(string.format("OAuth: -> %s %s", operation, endpoint))
  local ok, body, headers = LrTasks.pcall(function()
    return LrHttp.post(issuer .. endpoint, Http.formEncode(params), {
      { field = "Content-Type", value = "application/x-www-form-urlencoded" },
      { field = "Accept", value = "application/json" },
      { field = "Cookie", value = "" },
    }, "POST", 60)
  end)
  if not ok then
    logException(operation, body)
    return 0, nil, { representation = "exception" }
  end
  local info = Http.responseInfo(headers)
  local value
  local rawContentType = info.headers["content-type"] and info.headers["content-type"][1]
  local contentType = rawContentType and rawContentType:lower():match("^%s*([^;%s]+)")
  local isJSON = contentType == "application/json" or (contentType and contentType:match("^application/[%w.+-]+%+json$"))
  info.representation = contentType and "nonJSON" or "missingContentType"
  if isJSON then
    local decoded, result = pcall(JSON.decode, body or "")
    if decoded and JSON.isObject(result) then
      value = result
      info.representation = "object"
    else
      info.representation = decoded and "notObject" or "malformedJSON"
    end
  end
  -- Restrict correlation IDs to the usual hexadecimal/UUID forms. Neither raw
  -- response bodies, error descriptions nor arbitrary header values are logged.
  local requestId = info.request_id
  if type(requestId) ~= "string" or #requestId > 64 or not requestId:match("^[%x-]+$") then
    requestId = "unavailable"
  end
  local network = info.network_error or {}
  local native = tonumber(network.nativeCode)
  log_trace(string.format(
    "OAuth: <- %s HTTP %d; response=%s; error=%s; network=%s; native=%s; request_id=%s",
    operation,
    info.status,
    info.representation,
    value and value.error and errorCode(value.error) or "none",
    Http.networkErrorCode(info),
    native and tostring(native) or "none",
    requestId
  ))
  return info.status, value, info
end

function OAuth.connection(clientId, id, issuer, resource)
  if type(clientId) ~= "string" or clientId == "" or type(id) ~= "string" or id == "" then
    return nil
  end
  local key = "oauth2:" .. JSON.encode({ issuer, resource, clientId, id })
  if connections[key] then
    return connections[key]
  end
  local connection, record, busy, failure = {}, nil, false, nil
  local function store(value)
    local ok = LrTasks.pcall(function()
      LrPasswords.store(key, JSON.encode(value))
    end)
    if not ok then
      log_trace("OAuth: secure credential write failed")
      failure = storageMessage()
      return nil, failure
    end
    record = value
    log_trace("OAuth: secure credential write succeeded; state=" .. (value.reconnect_required and "refreshPending" or value.access_token and "connected" or "disconnected"))
    return true
  end
  local function locked(action)
    while busy do
      LrTasks.sleep(0.1)
    end
    busy = true
    local ok, first, second = LrTasks.pcall(action)
    busy = false
    if not ok then
      logException("connection operation", first)
      failure = reconnectMessage()
      return nil, failure
    end
    return first, second
  end
  local function valid(value)
    return type(value) == "table"
      and type(value.access_token) == "string"
      and value.access_token ~= ""
      and not value.access_token:find("[%s%c]")
      and type(value.refresh_token) == "string"
      and value.refresh_token ~= ""
      and type(value.expires_at) == "number"
      and value.reconnect_required ~= true
  end
  local function saveTokens(status, value, info)
    local problem
    if status ~= 200 then
      problem = "httpStatus"
    elseif not value then
      problem = info and info.representation or "missingObject"
    elseif type(value.token_type) ~= "string" or value.token_type:lower() ~= "bearer" then
      problem = "tokenType"
    elseif type(value.expires_in) ~= "number" or value.expires_in <= 0 or value.expires_in == math.huge then
      problem = "expiresIn"
    end
    if problem then
      log_trace("OAuth: token response rejected; reason=" .. problem)
      return nil, tokenFailure(status, value)
    end
    local now = LrDate.currentTime()
    local replacement = {
      access_token = value.access_token,
      refresh_token = value.refresh_token,
      expires_at = now + value.expires_in,
      -- Refresh slightly early for clock/transport skew, without assuming any
      -- server token lifetime or immediately expiring short-lived tokens.
      refresh_at = now + value.expires_in - math.min(60, value.expires_in * 0.1),
    }
    if not valid(replacement) then
      log_trace("OAuth: token response rejected; reason=credentials")
      return nil, tokenFailure(status, value)
    end
    if not store(replacement) then
      -- Do not leave a new connection live when its rotated credentials cannot
      -- be saved. Revocation is best-effort and is never retried.
      post(issuer, "/oauth/revoke", { client_id = clientId, token = replacement.refresh_token })
      return nil, storageMessage()
    end
    failure = nil
    log_trace("OAuth: replacement token pair saved")
    return replacement.access_token
  end
  function connection.isConnected()
    return not failure and valid(record) or false
  end
  function connection.getAccessToken(rejected)
    return locked(function()
      if failure then
        return nil, failure
      end
      if not valid(record) then
        log_trace("OAuth: no usable stored token pair; reconnect required")
        return nil, reconnectMessage()
      end
      local refreshAt = record.refresh_at or record.expires_at
      if refreshAt > LrDate.currentTime() and record.access_token ~= rejected then
        return record.access_token
      end
      local refresh = record.refresh_token
      -- One secure-store write replaces the entire token pair. Persist the
      -- consumed/in-flight state before sending, including across LR restarts.
      if not store({ reconnect_required = true, refresh_token = refresh }) then
        return nil, storageMessage()
      end
      local status, value, info = post(issuer, "/oauth/token", { grant_type = "refresh_token", client_id = clientId, refresh_token = refresh, resource = resource })
      return saveTokens(status, value, info)
    end)
  end
  function connection.reject(token)
    return locked(function()
      if record and record.access_token == token then
        return store({ reconnect_required = true, refresh_token = record.refresh_token })
      end
      return true
    end)
  end
  local function clearCredentials()
    local token = record and (record.refresh_token or record.access_token)
    local status = 200
    if token then
      status = post(issuer, "/oauth/revoke", { client_id = clientId, token = token })
    end
    local saved, message = store({})
    if not saved then
      return nil, message
    end
    failure = nil
    if status ~= 200 then
      return nil, LOC("$$$/PhotoDeck/OAuth/RevokeFailed=Disconnected locally, but PhotoDeck could not confirm revocation. Check Connected apps on PhotoDeck.")
    end
    return true
  end
  function connection.disconnect()
    if connection.pending then
      connection.pending.canceled = true
    end
    return locked(clearCredentials)
  end
  function connection.authorize(cancel)
    if connection.pending then
      return nil, LOC("$$$/PhotoDeck/OAuth/InProgress=PhotoDeck authorization is already in progress")
    end
    local attempt = {}
    connection.pending = attempt
    log_trace("OAuth: starting browser authorization")
    local ok, token, errorMessage = LrTasks.pcall(function()
      local disconnected, message = locked(clearCredentials)
      if not disconnected then
        return nil, message
      end
      local data, message = PKCE.generate()
      if not data then
        return nil, message
      end
      if attempt.canceled then
        return nil, reconnectMessage()
      end
      attempt.state, attempt.issuer, attempt.deadline = data.state, issuer, LrDate.currentTime() + browserTimeout
      pending[data.state] = attempt
      LrHttp.openUrlInBrowser(issuer .. "/oauth/authorize?" .. Http.formEncode({
        response_type = "code",
        client_id = clientId,
        redirect_uri = OAuth.redirectURI,
        code_challenge_method = "S256",
        code_challenge = data.challenge,
        scope = "api",
        resource = resource,
        state = data.state,
      }))
      while not attempt.callback and not attempt.canceled and LrDate.currentTime() < attempt.deadline do
        if cancel and cancel() then
          attempt.canceled = true
          break
        end
        LrTasks.sleep(0.1)
      end
      if attempt.canceled or (attempt.callback and attempt.callback.error == "access_denied") then
        return nil, LOC("$$$/PhotoDeck/OAuth/Canceled=PhotoDeck authorization canceled")
      end
      if not attempt.callback then
        return nil, LOC("$$$/PhotoDeck/OAuth/TimedOut=PhotoDeck authorization timed out. Please try again.")
      end
      if attempt.callback.error then
        log_trace("OAuth: authorization callback rejected; error=" .. errorCode(attempt.callback.error))
        return nil, authorizationFailure(attempt.callback, data)
      end
      -- Remove the callback before yielding to the token exchange: codes are
      -- single-use, even when the OS delivers the same URL more than once.
      pending[data.state] = nil
      return locked(function()
        if attempt.canceled or (cancel and cancel()) then
          return nil, LOC("$$$/PhotoDeck/OAuth/Canceled=PhotoDeck authorization canceled")
        end
        local status, value, info = post(issuer, "/oauth/token", {
          grant_type = "authorization_code",
          client_id = clientId,
          code = attempt.callback.code,
          redirect_uri = OAuth.redirectURI,
          code_verifier = data.verifier,
          resource = resource,
        })
        return saveTokens(status, value, info)
      end)
    end)
    if attempt.state then
      pending[attempt.state] = nil
    end
    connection.pending = nil
    if not ok then
      logException("browser authorization", token)
      return nil, reconnectMessage()
    end
    if attempt.canceled or (cancel and cancel()) then
      if token then
        connection.disconnect()
      end
      return nil, LOC("$$$/PhotoDeck/OAuth/Canceled=PhotoDeck authorization canceled")
    end
    return token and true or nil, errorMessage
  end
  connections[key] = connection
  -- Publish the controller before the first potentially yielding store read, so
  -- simultaneous callers always share its refresh lock.
  busy = true
  local loaded, text = LrTasks.pcall(function()
    return LrPasswords.retrieve(key)
  end)
  if not loaded then
    log_trace("OAuth: secure credential read failed")
    failure = storageMessage()
  elseif text and text ~= "" then
    local ok, value = pcall(JSON.decode, text)
    if ok and JSON.isObject(value) then
      record = value
    else
      log_trace("OAuth: stored credentials are malformed")
      failure = reconnectMessage()
    end
  end
  busy = false
  return connection
end

-- Called by Lightroom's URL handler. Never log callback URLs. Only validated
-- error descriptions may be shown as plain text; unsolicited callbacks are ignored.
function OAuth.receiveCallback(url)
  log_trace("OAuth: authorization callback received")
  if type(url) ~= "string" then
    log_trace("OAuth: callback ignored; reason=invalidType")
    return false
  end
  url = url:gsub('^"(.*)"$', "%1")
  if url:sub(1, #OAuth.redirectURI + 1) ~= OAuth.redirectURI .. "?" or url:find("[#\r\n]") or #url > 16384 then
    log_trace("OAuth: callback ignored; reason=invalidURL")
    return false
  end
  local params = {}
  local function unescape(value)
    if value:gsub("%%[0-9a-fA-F][0-9a-fA-F]", ""):find("%%") then
      return nil
    end
    return (value:gsub("+", " "):gsub("%%(%x%x)", function(hex)
      return string.char(tonumber(hex, 16))
    end))
  end
  for field in url:sub(#OAuth.redirectURI + 2):gmatch("[^&]+") do
    local key, value = field:match("^([^=]+)=(.*)$")
    key, value = key and unescape(key), value and unescape(value)
    if not key or not value or params[key] or key:find("%c") or value:find("%c") then
      log_trace("OAuth: callback ignored; reason=malformedParameters")
      return false
    end
    params[key] = value
  end
  local attempt = pending[params.state]
  if not attempt or attempt.callback or attempt.canceled or LrDate.currentTime() >= attempt.deadline then
    log_trace("OAuth: callback ignored; reason=noActiveAttempt")
    return false
  end
  if params.iss ~= attempt.issuer then
    log_trace("OAuth: callback ignored; reason=issuerMismatch")
    return false
  end
  if (not params.error and (not params.code or params.code == "")) or (params.error and params.code) then
    log_trace("OAuth: callback ignored; reason=invalidResult")
    return false
  end
  attempt.callback = params
  log_trace("OAuth: callback accepted; result=" .. (params.error and errorCode(params.error) or "code"))
  return true
end

return OAuth
