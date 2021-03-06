local LrDate = import 'LrDate'
local LrDigest = import 'LrDigest'
local LrFileUtils = import 'LrFileUtils'
local LrHttp = import 'LrHttp'
local LrPathUtils = import 'LrPathUtils'
local LrStringUtils = import 'LrStringUtils'
local LrXml = import 'LrXml'
local LrDialogs = import 'LrDialogs'
local LrTasks = import 'LrTasks'
local LrView = import 'LrView'
local PhotoDeckUtils = require 'PhotoDeckUtils'
local PhotoDeckAPIXSLT = require 'PhotoDeckAPIXSLT'

local logger = import 'LrLogger'( 'PhotoDeckPublishLightroomPlugin' )
logger:enable('logfile')
local log_info, log_trace, log_error = logger:quick('info', 'trace', 'error')

local PhotoDeckAPI_BASEURL = 'https://api.photodeck.com'
local PhotoDeckMY_BASEURL = 'https://my.photodeck.com'

local PhotoDeckAPI_SESSIONCOOKIE = '_ficelle_session'

local PhotoDeckAPI_KEY = ''
local PhotoDeckAPI_SECRET = ''

local isString = PhotoDeckUtils.isString
local isTable = PhotoDeckUtils.isTable
local printTable = PhotoDeckUtils.printTable

local PhotoDeckAPI = {
  hasDistributionKeys = PhotoDeckAPI_KEY and PhotoDeckAPI_KEY ~= '',
  key = '',
  secret = '',
  password = '',
  loggedin = false,
  otpEnabled = false,
  otp = nil,
  sessionCookie = nil,
  canSynchronize = true
}

local PhotoDeckAPICache = {}
local canRequestUploadLocation = true

-- sign API request according to docs at
-- http://www.photodeck.com/developers/get-started/
local function sign(method, uri, querystring)
  querystring = querystring or ''
  local cocoatime = LrDate.currentTime()
  local timestamp = LrDate.timeToW3CDate(cocoatime)

  local request = string.format('%s\n%s\n%s\n%s\n%s\n', method, uri,
                                querystring, PhotoDeckAPI.secret, timestamp)
  local signature = PhotoDeckAPI.key .. ':' .. LrDigest.SHA1.digest(request)
  -- log_trace(timestamp)
  -- log_trace(signature)
  return {
    { field = 'X-PhotoDeck-TimeStamp', value=timestamp },
    { field = 'X-PhotoDeck-Authorization', value=signature },
  }
end

local function auth_headers(method, uri, querystring)
  -- sign request
  local headers = sign(method, uri, querystring)

  -- set login cookies
  if PhotoDeckAPI.username and PhotoDeckAPI.password and not PhotoDeckAPI.loggedin then
    -- not logged in, send HTTP Basic credentials
    local password = PhotoDeckAPI.password;
    if PhotoDeckAPI.otpEnabled and PhotoDeckAPI.otp then
      password = password .. '+' .. PhotoDeckAPI.otp
      PhotoDeckAPI.otp = nil -- valid only once
    end
    local authorization = 'Basic ' .. LrStringUtils.encodeBase64(PhotoDeckAPI.username ..
      ':' .. password)
    table.insert(headers, { field = 'Authorization',  value = authorization })

  elseif PhotoDeckAPI.sessionCookie then
    -- already logged in, inject last known session cookie.
    -- NOTE: Lightroom usually does this by itself, but some installations seems to loose cookies between calls. So we are doing this manually.
    local cookie = PhotoDeckAPI_SESSIONCOOKIE .. '=' .. PhotoDeckAPI.sessionCookie
    table.insert(headers, { field = 'Cookie',  value = cookie })
  end

  return headers
end

-- extra chars from http://tools.ietf.org/html/rfc3986#section-2.2
local function urlencode(s)
  s = string.gsub(s, "([][:/?#@!#'()*,;&=+%%%c])", function (c)
    return string.format("%%%02X", string.byte(c))
  end)
  s = string.gsub(s, " ", "+")
  return s
end

local function urlencode_strict(s) -- also url-encodes accents
  s = string.gsub(s, "([^0-9A-Za-z])", function (c)
    return string.format("%%%02X", string.byte(c))
  end)
  s = string.gsub(s, " ", "+")
  return s
end

--- convert lua table to url encoded data
--- from http://www.lua.org/pil/20.3.html
local function table_to_querystring(data)
  assert(PhotoDeckUtils.isTable(data))

  local s = ""
  for k,v in pairs(data) do
    s = s .. "&" .. urlencode(k) .. "=" .. urlencode_strict(v)
  end
  return string.sub(s, 2)     -- remove first `&'
end

local function table_to_urlencoded_body(data)
  assert(PhotoDeckUtils.isTable(data))

  local s = ""
  for k,v in pairs(data) do
    s = s .. "&" .. urlencode(k) .. "=" .. urlencode(v)
  end
  return string.sub(s, 2)     -- remove first `&'
end

-- Makes sure that we don't call the API more than once every second starting from the 5th request in a row.
-- This is done to avoid hitting rate limits on the PhotoeDeck API and throwing errors
local ratelimit_aggressiveness = 10
local ratelimit_capacity = 60
local ratelimit_remaining = ratelimit_capacity
local ratelimit_window = 60
local ratelimit_reset = 60
local ratelimit_wait_until = 0

local function ratelimit_throttle()
  local wait_time = ratelimit_wait_until - LrDate.currentTime()
  if wait_time > 0 then
    log_trace(string.format('       ** Sleeping for %.2f seconds to keep within rate limiting quota', wait_time))
    LrTasks.sleep(wait_time)
  else
    ratelimit_wait_until = 0
  end
end

local function ratelimit_delay_next(seconds)
  local wait_until = LrDate.currentTime() + seconds
  if wait_until > ratelimit_wait_until then
    ratelimit_wait_until = wait_until
  end
end

local function ratelimit_update_limits(resp_headers)
  local limit
  local remaining
  local reset
  local retry_after
  local wait_for = 0

  for _, v in ipairs(resp_headers) do
    if isTable(v) then
      if v.field == 'ratelimit-limit' then
        limit = v.value
      elseif v.field == 'ratelimit-remaining' then
        remaining = v.value
      elseif v.field == 'ratelimit-reset' then
        reset = v.value
      elseif v.field == 'retry-after' then
        retry_after = v.value
      end
    end
  end

  if isString(reset) then
    reset = tonumber(reset)
    if reset and reset <= 120 then
      ratelimit_reset = reset
      ratelimit_window = reset
    end
  end

  if isString(limit) then
    local capacity = string.gsub(limit, ",.+", "")

    if capacity then
      capacity = tonumber(capacity)
      if capacity then
        ratelimit_capacity = capacity
        local max_window = 0

        for param in string.gmatch(limit, ", ([^,]+)") do
          local quota = string.gsub(param, ";.+", "")
          if quota then
            quota = tonumber(quota)
            if quota then
              for j in string.gmatch(param, ";([^;]*)") do
                local k,v,_
                _, _, k, v = string.find(j, "(%a+)=(.*)")
                if k == "w" then
                  v = tonumber(v)
                  if quota <= capacity and v and v > max_window then
                    max_window = v
                  end
                end
              end
            end
          end
        end

        if max_window > 0 then
          ratelimit_window = max_window
        end
      end
    end
  end

  if isString(remaining) then
    remaining = tonumber(remaining)
    if remaining then
      ratelimit_remaining = remaining
      if ratelimit_remaining <= 0 then
        wait_for = ratelimit_reset
      elseif ratelimit_capacity <= 0 then
        wait_for = ratelimit_window
      elseif ratelimit_remaining < ratelimit_capacity then
        wait_for = (((ratelimit_capacity - ratelimit_remaining) / ratelimit_capacity) ^ ratelimit_aggressiveness) * ratelimit_window
      end
      log_trace(string.format('       ** Rate limiting: remaining: %i/%i, window: %i, reset: %i, aggressiveness: %i, wait before next request: %.2f seconds', ratelimit_remaining, ratelimit_capacity, ratelimit_window, ratelimit_reset, ratelimit_aggressiveness, wait_for))
    end
  end

  if isString(retry_after) then
    retry_after = tonumber(retry_after)
    if (retry_after and retry_after <= 120) then
      wait_for = retry_after
      log_trace(string.format('       ** Retry after %.2f seconds', wait_for))
    end
  end

  if wait_for > 0 then
    ratelimit_delay_next(wait_for)
  end
end

local function handle_response(seq, response, resp_headers, onerror)
  local status
  local request_id

  for _, v in ipairs(resp_headers) do
    if isTable(v) then
      v.field = v.field:lower() -- lowercase all headers (HTTP/2 does this by default, but headers received via HTTP/1 are not lowercased)

      if v.field == 'status' then
        status = v
      elseif v.field == 'x-request-id' then
        request_id = v
      end
    end
  end

  local error_msg = nil
  local status_code = "999"

  if request_id then
    request_id = request_id.value
  else
    request_id = "No request ID"
  end
  if resp_headers.status then
    -- Get HTTP response code
    status_code = tostring(resp_headers.status)
  end

  if status then
    -- Get status from Status header, if any
    status_code = string.sub(status.value, 1, 3)
  end

  ratelimit_update_limits(resp_headers)

  if status_code >= "400" then
    if status then
      -- Get error from Status header
      error_msg = status.value
      if status_code == "429" then
        -- Too Many Requests. Wait until next request.
        -- Note: this HTTP error seems to be filtered out on LR/Windows at a lower level (the error will get catched in the status_code = "999" case)
        ratelimit_delay_next(ratelimit_reset)
      end
    else
      -- Generic HTTP error
      if status_code == "999" then
        error_msg = LOC("$$$/PhotoDeck/API/UnknownError=Unknown error")
      else
        error_msg = LOC("$$$/PhotoDeck/API/HTTPError=HTTP error ^1", status_code)
      end
    end

    if not response and status_code == "999" then
      error_msg = LOC("$$$/PhotoDeck/API/NoResponse=No response from network")
      -- No network connection, or we are blocked. Wait until next request.
      ratelimit_delay_next(ratelimit_reset)
    end

    local error_from_xml = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.error)
    if error_from_xml and error_from_xml.error and error_from_xml.error ~= "" then
      -- We got an error from the API, use that error message instead
      error_msg = error_from_xml.error
    end

    if onerror and onerror[status_code] then
      log_trace(string.format(' %s <- %s [%s] (handled by onerror)', seq, status_code, request_id))
      return onerror[status_code]()
    end

    --log_error("Bad response: " .. error_msg .. " => " .. (response or "(no response)"))
    --if resp_headers then
    --  log_error(PhotoDeckUtils.printLrTable(resp_headers))
    --end
    if status_code == "401" then
      PhotoDeckAPI.loggedin = false
      PhotoDeckAPI.sessionCookie = nil
      if error_from_xml then
        if error_from_xml.authmethod == 'basic:password+otp' then
          if PhotoDeckAPI.otpEnabled then
            log_error(string.format(' %s <- %s [%s]: %s (%s)', seq, status_code, request_id, error_msg, 'Incorrect OTP code'))
          else
            log_trace(string.format(' %s <- %s [%s]: %s (%s)', seq, status_code, request_id, error_msg, 'Retrying with OTP'))
            PhotoDeckAPI.otpEnabled = true
          end

          local f = LrView.osFactory();
          local c = f:column {
            spacing = f:dialog_spacing(),
            bind_to_object = PhotoDeckAPI,
            f:row {
              f:static_text {
                title = LOC "$$$/PhotoDeck/OTPDialog/Code=Authentication code:",
                alignment = 'right'
              },
              f:edit_field {
                value = LrView.bind 'otp',
                immediate = false,
                width_in_chars = 6
              }
            }
          }
          local otp_res = LrDialogs.presentModalDialog {
            title = LOC "$$$/PhotoDeck/OTPDialog/Title=PhotoDeck Two-factor authentication",
            contents = c
          }
          if otp_res == 'ok' then
            return 'retry', error_msg
          end

        elseif error_from_xml.authmethod == 'basic' then
          if PhotoDeckAPI.otpEnabled then
            log_trace(string.format(' %s <- %s [%s]: %s (%s)', seq, status_code, request_id, error_msg, 'Retrying without OTP'))
            PhotoDeckAPI.otpEnabled = false
            return 'retry', error_msg
          end
        end
      end
    end
    if status_code == "999" then
      PhotoDeckAPI.loggedin = false
      PhotoDeckAPI.sessionCookie = nil
      log_error(string.format(' %s <- %s [%s]: %s %s', seq, status_code, request_id, error_msg, printTable(resp_headers)))
    else
      log_error(string.format(' %s <- %s [%s]: %s', seq, status_code, request_id, error_msg))
    end
  else
    PhotoDeckAPI.loggedin = true
    log_trace(string.format(' %s <- %s [%s]', seq, status_code, request_id))

    -- Try to extract session cookie. We will reinject it later, as it seems that some Lightroom installations loose cookies between calls.
    for _, set_cookie in ipairs(PhotoDeckUtils.filter(resp_headers, function(v) return isTable(v) and v.field == 'set-cookie' end)) do
      local parsed_cookies = LrHttp.parseCookie(set_cookie.value, false)
      if parsed_cookies[PhotoDeckAPI_SESSIONCOOKIE] then
        PhotoDeckAPI.sessionCookie = parsed_cookies[PhotoDeckAPI_SESSIONCOOKIE]
      end
    end
  end

  return response, error_msg
end

-- make HTTP GET request to PhotoDeck API
-- must be called within an LrTask
function PhotoDeckAPI.request(method, uri, data, onerror)
  local querystring = ''
  local body = ''
  local error_msg
  if data then
    if method == 'GET' then
      querystring = table_to_querystring(data)
    else
      body = table_to_urlencoded_body(data)
    end
  end

  -- set up authorisation headers
  local headers = auth_headers(method, uri, querystring)

  -- build full url
  local fullurl = PhotoDeckAPI_BASEURL .. uri
  if querystring and querystring ~= '' then
    fullurl = fullurl .. '?' .. querystring
  end

  -- call API
  ratelimit_throttle()
  local result, resp_headers
  local seq = string.format("%5i", math.random(99999))
  if method == 'GET' then
    log_trace(string.format(' %s -> %s %s', seq, method, fullurl))
    result, resp_headers = LrHttp.get(fullurl, headers)
  else
    -- override default Content-Type!
    log_trace(string.format(' %s -> %s %s\n%s', seq, method, fullurl, body))
    table.insert(headers, { field = 'Content-Type',  value = 'application/x-www-form-urlencoded'})
    result, resp_headers = LrHttp.post(fullurl, body, headers, method)
  end

  result, error_msg = handle_response(seq, result, resp_headers, onerror)

  if result == 'retry' then
    return PhotoDeckAPI.request(method, uri, data, onerror)
  end

  return result, error_msg
end

function PhotoDeckAPI.requestMultiPart(method, uri, content, onerror)
  local error_msg
  local seq = string.format("%5i", math.random(99999))
  log_trace(string.format(' %s -> %s[multipart] %s', seq, method, uri))

  if method ~= "POST" then
    -- LrHttp doesn't implement non-POSTs multipart requests:
    -- POST content but pass the correct method to the PhotoDeck API as a field
    table.insert(content, { name = "_method", value = method })
    method = "POST"
  end

  -- set up authorisation headers
  local headers = auth_headers(method, uri)
  -- build full url
  local fullurl = PhotoDeckAPI_BASEURL .. uri

  -- call API
  ratelimit_throttle()
  local result, resp_headers
  result, resp_headers = LrHttp.postMultipart(fullurl, content, headers)

  result, error_msg = handle_response(seq, result, resp_headers, onerror)

  if result == 'retry' then
    return PhotoDeckAPI.requestMultiPart(method, uri, content, onerror)
  end

  return result, error_msg
end

function PhotoDeckAPI.connect(key, secret, username, password)
  if PhotoDeckAPI.hasDistributionKeys then
    -- use builtin keys
    PhotoDeckAPI.key = PhotoDeckAPI_KEY
    PhotoDeckAPI.secret = PhotoDeckAPI_SECRET
  else
    -- use the one supplied by the user
    PhotoDeckAPI.key = key
    PhotoDeckAPI.secret = secret
  end

  if PhotoDeckAPI.loggedin and (PhotoDeckAPI.username ~= username or PhotoDeckAPI.password ~= password) then
    PhotoDeckAPI.logout()
  end

  PhotoDeckAPI.username = username
  PhotoDeckAPI.password = password
end

function PhotoDeckAPI.ping(text)
  log_trace('PhotoDeckAPI.ping()')
  local t = {}
  if text then
    t = { text = text }
  end
  local response, error_msg = PhotoDeckAPI.request('GET', '/ping.xml', t)
  local result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.ping)
  return result, error_msg
end

function PhotoDeckAPI.logout()
  log_trace('PhotoDeckAPI.logout()')
  local response, error_msg = PhotoDeckAPI.request('GET', '/logout.xml')
  PhotoDeckAPI.loggedin = false
  PhotoDeckAPI.sessionCookie = nil
  return response, error_msg
end

function PhotoDeckAPI.whoami()
  log_trace('PhotoDeckAPI.whoami()')
  local response, error_msg = PhotoDeckAPI.request('GET', '/whoami.xml')
  local result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.user)
  if not result or not result.email or result.email == '' then
    PhotoDeckAPI.loggedin = false
    PhotoDeckAPI.sessionCookie = nil
  end
  -- log_trace(printTable(result))
  return result, error_msg
end

function PhotoDeckAPI.websites()
  log_trace('PhotoDeckAPI.websites()')
  local cacheKey = 'websites/' .. PhotoDeckAPI.username
  local result = PhotoDeckAPICache[cacheKey]
  local response, error_msg = nil
  if not result then
    response, error_msg = PhotoDeckAPI.request('GET', '/websites.xml', { view = 'details' })
    result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.websites)
    if not error_msg then
      local websites_count = 0
      if result then
        for _ in pairs(result) do websites_count = websites_count + 1 end
      end
      if websites_count == 0 then
        error_msg = LOC("$$$/PhotoDeck/API/Websites/NotFound=No websites found")
      end
    end
    if error_msg then
      PhotoDeckAPI.loggedin = false
      PhotoDeckAPI.sessionCookie = nil
    else
      PhotoDeckAPICache[cacheKey] = result
    end
    -- log_trace(printTable(result))
  end
  return result, error_msg
end

function PhotoDeckAPI.website(urlname)
  local websites, error_msg = PhotoDeckAPI.websites()
  local website = nil
  if not error_msg then
    website = websites[urlname]
    if not website then
      error_msg = LOC("$$$/PhotoDeck/API/Website/NotFound=Website not found")
    end
  end
  return website, error_msg
end

function PhotoDeckAPI.galleries(urlname)
  log_trace(string.format('PhotoDeckAPI.galleries("%s")', urlname))
  local galleries
  local allgalleries = {}
  local response
  local error_msg = nil
  local page = 0
  local totalPages = 1
  local newTotalPages
  while not error_msg and page < totalPages do
    page = page + 1
    response, error_msg = PhotoDeckAPI.request('GET', '/websites/' .. urlname .. '/galleries.xml', { view = 'details', page = page, per_page = 500 })
    galleries = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.galleries)
    newTotalPages = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.totalPages)
    if newTotalPages and newTotalPages ~= "" then
      totalPages = tonumber(newTotalPages)
    end
    --log_trace("PhotoDeckAPI.galleries " .. tostring(page) .. "/" .. tostring(totalPages) .. ": " .. printTable(galleries))

    if not galleries and not error_msg then
      error_msg = LOC("$$$/PhotoDeck/API/Gallery/ErrorGettingGalleries=Couldn't get galleries")
    end

    if not error_msg then
      local added_count = 0
      for uuid, gallery in pairs(galleries) do
        added_count = added_count + 1
        allgalleries[uuid] = gallery
      end
      if added_count == 0 then break end
    end
  end
  if not error_msg then
    --log_trace("PhotoDeckAPI.galleries: " .. printTable(allgalleries))
    return allgalleries
  else
    return nil, error_msg
  end
end

function PhotoDeckAPI.gallery(urlname, galleryId, ignore_not_found)
  log_trace(string.format('PhotoDeckAPI.gallery("%s", "%s")', urlname, galleryId))
  local onerror = {}
  if ignore_not_found then
    onerror["404"] = function() return nil end
  end
  local response, error_msg = PhotoDeckAPI.request('GET', '/websites/' .. urlname .. '/galleries/' .. galleryId .. '.xml', { view = 'details' }, onerror)
  local result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.gallery)
  -- log_trace(printTable(result))
  return result, error_msg
end

function PhotoDeckAPI.openGalleryInBackend(galleryId)
  log_trace(string.format('PhotoDeckAPI.openGalleryInBackend("%s")', galleryId))
  LrHttp.openUrlInBrowser(PhotoDeckMY_BASEURL .. '/medias/manage?gallery_id=' .. galleryId)
end

local function buildGalleryInfoFromLrCollectionInfo(collectionInfo)
  local galleryInfo = {}
  galleryInfo['gallery[name]'] = collectionInfo.name
  local collectionSettings = collectionInfo.collectionSettings
  if collectionSettings then
    galleryInfo['gallery[description]'] = collectionSettings['description']
    galleryInfo['gallery[display_style]'] = collectionSettings['display_style']
  end
  return galleryInfo
end

function PhotoDeckAPI.createGallery(urlname, parentId, collectionInfo)
  log_trace(string.format('PhotoDeckAPI.createGallery("%s", "%s", <collectionInfo>)', urlname, parentId))
  local galleryInfo = buildGalleryInfoFromLrCollectionInfo(collectionInfo)
  galleryInfo['gallery[content_order]'] = 'manual-last'
  galleryInfo['gallery[parent]'] = parentId
  local response, error_msg = PhotoDeckAPI.request('POST', '/websites/' .. urlname .. '/galleries.xml', galleryInfo)
  local gallery = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.gallery)
  return gallery, error_msg
end

function PhotoDeckAPI.updateGallery(urlname, galleryId, parentId, collectionInfo)
  log_trace(string.format('PhotoDeckAPI.updateGallery("%s", "%s", "%s", <collectionInfo>)', urlname, galleryId, parentId))
  local galleryInfo = buildGalleryInfoFromLrCollectionInfo(collectionInfo)
  galleryInfo['gallery[parent]'] = parentId
  local response, error_msg = PhotoDeckAPI.request('PUT', '/websites/' .. urlname .. '/galleries/' .. galleryId .. '.xml', galleryInfo)
  local gallery = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.gallery)
  return gallery, error_msg
end


function PhotoDeckAPI.createOrUpdateGallery(urlname, collectionInfo, updateSettings)
  log_trace(string.format('PhotoDeckAPI.createOrUpdateGallery("%s", <collectionInfo>)', urlname))

  local website, error_msg = PhotoDeckAPI.website(urlname)
  if error_msg then
    return nil, error_msg
  end

  local collection = collectionInfo.publishedCollection

  -- Find PhotoDeck gallery (if PhotoDeck's ID is known)
  local gallery = nil
  local galleryId = collection:getRemoteId()
  if galleryId then
    -- find by remote ID if known
    gallery = PhotoDeckAPI.gallery(urlname, galleryId, true)
  end

  -- Find PhotoDeck parent gallery ID
  local parentGalleryId = nil
  local parentGalleryMissingId = nil
  local parentJustCreated = false

  parentGalleryId = website.rootgalleryuuid
  if not parentGalleryId or parentGalleryId == "" then
    return nil, LOC("$$$/PhotoDeck/API/Galleries/RootNotFound=Couldn't find PhotoDeck root gallery")
  end
  for _, parent in pairs(collectionInfo.parents) do
    parentGalleryId = parent.remoteCollectionId
  end
  if gallery and parentGalleryId ~= gallery.parentuuid then
    -- not properly connected?
    parentGalleryId = nil
  end

  local lookupIntoParents = not gallery or not parentGalleryId
  local iterations = 0
  while lookupIntoParents do
    iterations = iterations + 1
    if iterations > 2 then -- prevent endless loops, we are not supposed to do this more than twice
      return nil, error_msg or LOC("$$$/PhotoDeck/API/Gallery/SubGalleryNotFound=Couldn't get subgallery")
    end
    lookupIntoParents = false
    parentJustCreated = false

    if not parentGalleryId then
      -- PhotoDeck parent gallery ID unknown: find all parent galleries, create
      -- if missing and connect them to Lightroom if not already done

      -- Start from the root gallery
      parentGalleryId = website.rootgalleryuuid

      -- Now iterate over each parent, starting from the top level
      for _, parent in pairs(collectionInfo.parents) do
        local parentGallery = nil
        local parentId = parent.remoteCollectionId
        if parentId and parentId ~= parentGalleryMissingId then
          -- find by remote ID if known
          parentGallery = PhotoDeckAPI.gallery(urlname, parentId, true)
        end
        if not parentGallery and not parentJustCreated then
          -- not found, search by name within subgalleries present in our parent
          -- (unless we have just created this gallery, in which case we assume that it's empty)
          local subgalleries, error_msg = PhotoDeckAPI.subGalleriesInGallery(urlname, parentGalleryId, parent.name)
          if error_msg then
            return nil, error_msg
          end
          for uuid, subgallery in pairs(subgalleries) do
            if subgallery.name == parent.name then
              parentGallery, error_msg = PhotoDeckAPI.gallery(urlname, uuid)
              if error_msg or not parentGallery then
                return nil, error_msg or LOC("$$$/PhotoDeck/API/Gallery/SubGalleryNotFound=Couldn't get subgallery")
              end
              break
            end
          end
        end
        if not parentGallery then
          -- not found, create
          parentGallery, error_msg = PhotoDeckAPI.createGallery(urlname, parentGalleryId, { name = parent.name })
          if error_msg then
            return nil, error_msg
          end
          parentJustCreated = true
        else
          parentJustCreated = false
        end
        local parentCollection = collection.catalog:getPublishedCollectionByLocalIdentifier(parent.localCollectionId)
        parentGallery.fullurl = website.homeurl .. "/-/" .. parentGallery.fullurlpath
        if parentCollection and (not parent.remoteCollectionId or parentCollection:getRemoteId() ~= parentGallery.uuid or parentCollection:getRemoteUrl() ~= parentGallery.fullurl) then
          --log_trace('Updating parent remote Id and Url')
          parentCollection.catalog:withWriteAccessDo('Set Parent Remote Id and Url', function()
            parentCollection:setRemoteId(parentGallery.uuid)
            parentCollection:setRemoteUrl(parentGallery.fullurl)
          end)
        end

        parentGalleryId = parentGallery.uuid -- our parent gallery is now this one
      end
    end

    -- New gallery, PhotoDeck ID not known or deleted from PhotoDeck, but we
    -- know the PhotoDeck ID of the parent: try to find gallery by name under
    -- its parent (unless it has just been created, in which case we can assume
    -- that it's empty)
    if not gallery and not parentJustCreated then
      local subgalleries, error_msg = PhotoDeckAPI.subGalleriesInGallery(urlname, parentGalleryId, collectionInfo.name, iterations == 1)
      if error_msg then
        return nil, error_msg
      end
      if subgalleries then
        for uuid, subgallery in pairs(subgalleries) do
          if subgallery.name == collectionInfo.name then
            gallery, error_msg = PhotoDeckAPI.gallery(urlname, uuid)
            if error_msg or not gallery then
              return nil, error_msg or LOC("$$$/PhotoDeck/API/Gallery/SubGalleryNotFound=Couldn't get subgallery")
            end
            break
          end
        end
      else
        -- we need to recreate the parent, it's missing
        parentGalleryMissingId = parentGalleryId
        parentGalleryId = nil
        lookupIntoParents = true
      end
    end
  end

  if gallery then
    -- PhotoDeck gallery found, update if necessary
    local changed = gallery.parentuuid ~= parentGalleryId or gallery.name ~= collectionInfo.name

    local collectionSettings
    local settingsChanged = false
    if updateSettings then
      -- User has edited the gallery settings (ie, description and/or display style), so update gallery if changed
      collectionSettings = collectionInfo.collectionSettings
      if collectionSettings then
        if not settingsChanged and collectionSettings['description'] and collectionSettings['description'] ~= '' then
          settingsChanged = collectionSettings['description'] ~= gallery.description
        end
        if not settingsChanged and collectionSettings['display_style'] and collectionSettings['display_style'] ~= '' then
          settingsChanged = collectionSettings['display_style'] ~= gallery.displaystyle
        end

        changed = changed or settingsChanged
      end
    end

    if changed then
      gallery, error_msg = PhotoDeckAPI.updateGallery(urlname, gallery.uuid, parentGalleryId, collectionInfo)

      if not error_msg and settingsChanged then
        -- resynchronize LR published collection settings with the actual data in PhotoDeck
        collectionSettings['description'] = gallery.description
        collectionSettings['display_style'] = gallery.displaystyle
        collection.catalog:withWriteAccessDo('Resynchronize LR collection settings', function()
          if collection:type() == 'LrPublishedCollection' then
            collection:setCollectionSettings(collectionSettings)
          elseif collection:type() == 'LrPublishedCollectionSet' then
            collection:setCollectionSetSettings(collectionSettings)
          end
        end)
      end
    end
  else
    -- PhotoDeck gallery not found, create
    gallery, error_msg = PhotoDeckAPI.createGallery(urlname, parentGalleryId, collectionInfo)
  end
  if error_msg then
    return gallery, error_msg
  end
  gallery.fullurl = website.homeurl .. "/-/" .. gallery.fullurlpath
  if collection:getRemoteId() == nil or collection:getRemoteId() ~= gallery.uuid or
    collection:getRemoteUrl() ~= gallery.fullurl then
    --log_trace('Updating collection remote Id and Url')
    collection.catalog:withWriteAccessDo('Set Remote Id and Url', function()
      collection:setRemoteId(gallery.uuid)
      collection:setRemoteUrl(gallery.fullurl)
    end)
  end
  return gallery
end

function PhotoDeckAPI.synchronizeGalleries(urlname, publishService, progressScope)
  log_trace(string.format('PhotoDeckAPI.synchronizeGalleries("%s", <publishService>, <progressScope>)', urlname))
  local catalog = publishService.catalog

  if not PhotoDeckAPI.canSynchronize then
    return nil, LOC("$$$/PhotoDeck/SynchronizeStatus/AlreadyInProgress=Task already in progress")
  end
  PhotoDeckAPI.canSynchronize = false

  local createCount = 0
  local deleteCount = 0
  local updateCount = 0
  local errorsCount = 0

  progressScope:setCaption(LOC("$$$/PhotoDeck/SynchronizeStatus/Connecting=Connecting to PhotoDeck website"))
  local website = PhotoDeckAPI.website(urlname)
  local rootGalleryId = nil
  if website then
    rootGalleryId = website.rootgalleryuuid
  end

  if not rootGalleryId then
    PhotoDeckAPI.canSynchronize = true
    return nil, LOC("$$$/PhotoDeck/API/Galleries/RootNotFound=Couldn't find PhotoDeck root gallery")
  end

  progressScope:setCaption(LOC("$$$/PhotoDeck/SynchronizeStatus/ReadingStructure=Reading PhotoDeck gallery structure"))
  local photodeckGalleries, error_msg = PhotoDeckAPI.galleries(urlname)

  if not photodeckGalleries or error_msg then
    PhotoDeckAPI.canSynchronize = true
    return nil, error_msg or LOC("$$$/PhotoDeck/SynchronizeStatus/ErrorReadingStructure=Couldn't get PhotoDeck gallery structure")
  end

  local photodeckGalleriesByParent = {}
  for uuid, gallery in pairs(photodeckGalleries) do
    if gallery.parentuuid == '' or not gallery.parentuuid then
      gallery.parentuuid = 'NONE'
    end
    if not photodeckGalleriesByParent[gallery.parentuuid] then
      photodeckGalleriesByParent[gallery.parentuuid] = {}
    end
    photodeckGalleriesByParent[gallery.parentuuid][uuid] = gallery
  end


  local synchronizeGallery
  synchronizeGallery = function(depth, parentPDGalleryUUID, parentLRCollectionSet)
    if progressScope:isCanceled() then return end

    local parentPDGallery = photodeckGalleries[parentPDGalleryUUID]
    progressScope:setCaption(LOC("$$$/PhotoDeck/SynchronizeStatus/Synchronizing=Synchronizing ^1", parentPDGallery.name))
    log_trace(string.format("SYNC: Exploring PhotoDeck galleries under %s '%s' at depth %i", parentPDGalleryUUID, parentPDGallery.name, depth))
    local pdGalleries = photodeckGalleriesByParent[parentPDGalleryUUID] or {}
    local lrCollectionSets = parentLRCollectionSet:getChildCollectionSets()
    local lrCollections = parentLRCollectionSet:getChildCollections()

    for uuid, gallery in pairs(pdGalleries) do
      gallery.fullurl = website.homeurl .. '/-/' .. gallery.fullurlpath
    end

    -- Scan Lightroom published collections, and connect them to PhotoDeck galleries
    local lrCollectionsByRemoteId = {}
    for _, lrCollection in pairs(lrCollections) do
      local rid = lrCollection:getRemoteId()
      if not rid or rid == '' then
        -- unconnected published collection, try to connect by name
        local lrCollectionName = lrCollection:getName()
        for uuid, gallery in pairs(pdGalleries) do
          if lrCollectionName == gallery.name then
            -- found matching gallery
            local collectionSettings = lrCollection:getCollectionInfoSummary().collectionSettings or {}
            collectionSettings.description = gallery.description
            collectionSettings.display_style = gallery.displaystyle
            catalog:withWriteAccessDo('Resynchronize LR collection settings', function()
              lrCollection:setRemoteId(uuid)
              lrCollection:setRemoteUrl(gallery.fullurl)
              lrCollection:setCollectionSettings(collectionSettings)
            end)
            updateCount = updateCount + 1
            rid = uuid
            break
          end
        end
      end

      local gallery = pdGalleries[rid]
      if not gallery or gallery.parentuuid ~= parentPDGalleryUUID then
        log_trace(string.format("SYNC: Lightroom Published Collection %i '%s' is connected to PhotoDeck gallery %s, but it doesn't exist anymore. Deleting Published Collection.", lrCollection.localIdentifier, lrCollection:getName(), rid or '(none)'))
        catalog:withWriteAccessDo('Deleting Published Collection', function()
          lrCollection:delete()
        end)
        deleteCount = deleteCount + 1
      elseif lrCollectionsByRemoteId[rid] then
        -- duplicate LR collections!
        local lrCollectionDup = lrCollectionsByRemoteId[rid]
        if gallery.name == lrCollectionDup:getName() then
          log_trace(string.format("SYNC: Lightroom Published Collection %i '%s' is connected to PhotoDeck gallery %s '%s', but we already have Published Collection %i '%s' connected to it. Deleting the former.", lrCollection.localIdentifier, lrCollection:getName(), rid, gallery.name, lrCollectionDup.localIdentifier, lrCollectionDup:getName()))
          catalog:withWriteAccessDo('Deleting Published Collection', function()
            lrCollection:delete()
          end)
          deleteCount = deleteCount + 1
        else
          log_trace(string.format("SYNC: Lightroom Published Collection %i '%s' is connected to PhotoDeck gallery %s '%s', but we already have Published Collection %i '%s' connected to it. Deleting the later.", lrCollection.localIdentifier, lrCollection:getName(), rid, gallery.name, lrCollectionDup.localIdentifier, lrCollectionDup:getName()))
          catalog:withWriteAccessDo('Deleting Published Collection', function()
            lrCollectionDup:delete()
          end)
          deleteCount = deleteCount + 1
        end
      else
        lrCollectionsByRemoteId[rid] = lrCollection
      end
    end

    -- Scan Lightroom published collections sets, and connect them to PhotoDeck galleries
    local lrCollectionSetsByRemoteId = {}
    for _, lrCollectionSet in pairs(lrCollectionSets) do
      local rid = lrCollectionSet:getRemoteId()
      if not rid or rid == '' then
        -- unconnected published collection, try to connect by name
        local lrCollectionSetName = lrCollectionSet:getName()
        for uuid, gallery in pairs(pdGalleries) do
          if lrCollectionSetName == gallery.name then
            -- found matching gallery
            local collectionSettings = lrCollectionSet:getCollectionSetInfoSummary().collectionSettings or {}
            collectionSettings.description = gallery.description
            collectionSettings.display_style = gallery.displaystyle
            catalog:withWriteAccessDo('Resynchronize LR collection settings', function()
              lrCollectionSet:setRemoteId(uuid)
              lrCollectionSet:setRemoteUrl(gallery.fullurl)
              lrCollectionSet:setCollectionSetSettings(collectionSettings)
            end)
            updateCount = updateCount + 1
            rid = uuid
            break
          end
        end
      end

      local gallery = pdGalleries[rid]
      if not gallery or gallery.parentuuid ~= parentPDGalleryUUID then
        log_trace(string.format("SYNC: Lightroom Published Collection Set %i '%s' is connected to PhotoDeck gallery %s, but it doesn't exist anymore. Deleting Published Collection Set.", lrCollectionSet.localIdentifier, lrCollectionSet:getName(), rid or '(none)'))
        catalog:withWriteAccessDo('Deleting Published Collection Set', function()
          lrCollectionSet:delete()
        end)
        deleteCount = deleteCount + 1
      elseif lrCollectionSetsByRemoteId[rid] then
        -- duplicate LR collections sets!
        local lrCollectionSetd = lrCollectionSetsByRemoteId[rid]
        if gallery.name == lrCollectionSetd:getName() then
          log_trace(string.format("SYNC: Lightroom Published Collection Set %i '%s' is connected to PhotoDeck gallery %s '%s', but we already have Published Collection Set %i '%s' connected to it. Deleting the former.", lrCollectionSet.localIdentifier, lrCollectionSet:getName(), rid, gallery.name, lrCollectionSetd.localIdentifier, lrCollectionSetd:getName()))
          catalog:withWriteAccessDo('Deleting Published Collection Set', function()
            lrCollectionSet:delete()
          end)
          deleteCount = deleteCount + 1
        else
          log_trace(string.format("SYNC: Lightroom Published Collection Set %i '%s' is connected to PhotoDeck gallery %s '%s', but we already have Published Collection Set %i '%s' connected to it. Deleting the later.", lrCollectionSet.localIdentifier, lrCollectionSet:getName(), rid, gallery.name, lrCollectionSetd.localIdentifier, lrCollectionSetd:getName()))
          catalog:withWriteAccessDo('Deleting Published Collection Set', function()
            lrCollectionSetd:delete()
          end)
          deleteCount = deleteCount + 1
        end
      else
        lrCollectionSetsByRemoteId[rid] = lrCollectionSet
      end
    end

    -- Find missing Lightroom published collections / collection sets
    for uuid, gallery in pairs(pdGalleries) do
      if progressScope:isCanceled() then return end

      local lrCollectionSet = lrCollectionSetsByRemoteId[uuid]
      local lrCollection = lrCollectionsByRemoteId[uuid]
      local shouldBeACollectionSet = photodeckGalleriesByParent[uuid]
      local shouldBeACollection = not shouldBeACollectionSet and gallery.mediascount and gallery.mediascount ~= '' and tonumber(gallery.mediascount) > 0

      if lrCollection and shouldBeACollectionSet then
        log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' is connected to Lightroom Published Collection %i, but it should be Publish Collection Set. Deleting Published Collection.", uuid, gallery.name, lrCollection.localIdentifier))
        catalog:withWriteAccessDo('Deleting Published Collection', function()
          lrCollection:delete()
        end)
        deleteCount = deleteCount + 1
        lrCollection = nil
      end

      if lrCollectionSet and shouldBeACollection then
        log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' is connected to Lightroom Published Collection Set %i, but it should be Publish Collection. Deleting Published Collection Set.", uuid, gallery.name, lrCollectionSet.localIdentifier))
        catalog:withWriteAccessDo('Deleting Published Collection Set', function()
          lrCollectionSet:delete()
        end)
        deleteCount = deleteCount + 1
        lrCollectionSet = nil
      end

      if lrCollection and lrCollectionSet then
        -- exists has both a Lightroom Published Collection and Published Collection Set. Choose the right type and delete the other.
        if shouldBeACollectionSet then
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' is connected to Lightroom Published Collection Set %i AND to Lightroom Published Collection %i, but it should be Publish Collection Set. Deleting Published Collection.", uuid, gallery.name, lrCollectionSet.localIdentifier, lrCollection.localIdentifier))
          catalog:withWriteAccessDo('Deleting Published Collection', function()
            lrCollection:delete()
          end)
          deleteCount = deleteCount + 1
          lrCollection = nil
        elseif shouldBeACollection then
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' is connected to Lightroom Published Collection Set %i AND to Lightroom Published Collection %i, but it should be Publish Collection. Deleting Published Collection Set.", uuid, gallery.name, lrCollectionSet.localIdentifier, lrCollection.localIdentifier))
          catalog:withWriteAccessDo('Deleting Published Collection Set', function()
            lrCollectionSet:delete()
          end)
          deleteCount = deleteCount + 1
          lrCollectionSet = nil
        else
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' is connected to Lightroom Published Collection Set %i AND to Lightroom Published Collection %i, and we don't know yet what it should be. Assuming Published Collection, and deleting Published Collection Set.", uuid, gallery.name, lrCollectionSet.localIdentifier, lrCollection.localIdentifier))
          catalog:withWriteAccessDo('Deleting Published Collection', function()
            lrCollection:delete()
          end)
          deleteCount = deleteCount + 1
          lrCollectionSet = nil
        end
      end

      if lrCollectionSet then
        -- Already properly connected, good
        local collectionSettings = lrCollectionSet:getCollectionSetInfoSummary().collectionSettings or {}
        if lrCollectionSet:getRemoteUrl() ~= gallery.fullurl
          or collectionSettings.description ~= gallery.description
          or collectionSettings.display_style ~= gallery.displaystyle then
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' already connected to Lightroom Published Collection Set %i. Updating.", uuid, gallery.name, lrCollectionSet.localIdentifier))
          collectionSettings.description = gallery.description
          collectionSettings.display_style = gallery.displaystyle
          catalog:withWriteAccessDo('Resynchronize LR collection settings', function()
            lrCollectionSet:setRemoteUrl(gallery.fullurl)
            lrCollectionSet:setCollectionSetSettings(collectionSettings)
          end)
          updateCount = updateCount + 1
        else
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' already connected to Lightroom Published Collection Set %i. Doing nothing.", uuid, gallery.name, lrCollectionSet.localIdentifier))
        end
      elseif lrCollection then
        -- Already properly connected, good
        local collectionSettings = lrCollection:getCollectionInfoSummary().collectionSettings or {}
        if lrCollection:getRemoteUrl() ~= gallery.fullurl
          or collectionSettings.description ~= gallery.description
          or collectionSettings.display_style ~= gallery.displaystyle then
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' already connected to Lightroom Published Collection %i. Updating.", uuid, gallery.name, lrCollection.localIdentifier))
          collectionSettings.description = gallery.description
          collectionSettings.display_style = gallery.displaystyle
          catalog:withWriteAccessDo('Resynchronize LR collection settings', function()
            lrCollection:setRemoteUrl(gallery.fullurl)
            lrCollection:setCollectionSettings(collectionSettings)
          end)
          updateCount = updateCount + 1
        else
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' already connected to Lightroom Published Collection %i. Doing nothing.", uuid, gallery.name, lrCollection.localIdentifier))
        end
      else
        -- Missing in Lightroom: create
        local collectionName = gallery.name

        -- Check for duplicate gallery names in this parent gallery: Lightroom does indeed require name uniqueness, but PhotoDeck doesn't
        local copyCount = 1
        for uuidN, galleryN in pairs(pdGalleries) do
          if galleryN.name == gallery.name then
            if uuid == uuidN then
              break
            else
              copyCount = copyCount + 1
            end
          end
        end
        if copyCount > 1 then
          collectionName = collectionName .. ' (' .. tostring(copyCount) .. ')'
        end

        if shouldBeACollectionSet then
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' NOT found in Lightroom. Creating Published Collection Set.", uuid, collectionName))
          catalog:withWriteAccessDo('Creating Published Collection Set', function()
            lrCollectionSet = publishService:createPublishedCollectionSet(collectionName, parentLRCollectionSet)
          end)
          if lrCollectionSet then
            local collectionSettings = {}
            collectionSettings.description = gallery.description
            collectionSettings.display_style = gallery.displaystyle
            catalog:withWriteAccessDo('Set LR collection settings', function()
              lrCollectionSet:setRemoteId(uuid)
              lrCollectionSet:setRemoteUrl(gallery.fullurl)
              lrCollectionSet:setCollectionSetSettings(collectionSettings)
            end)
            createCount = createCount + 1
          else
            log_trace(string.format("SYNC ERROR: PhotoDeck gallery %s '%s' NOT found in Lightroom, and failed to create Published Collection Set.", uuid, collectionName))
            errorsCount = errorsCount + 1
          end
        elseif shouldBeACollection then
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' NOT found in Lightroom. Creating Published Collection.", uuid, collectionName))
          catalog:withWriteAccessDo('Creating Published Collection', function()
            lrCollection = publishService:createPublishedCollection(collectionName, parentLRCollectionSet)
          end)
          if lrCollection then
            local collectionSettings = {}
            collectionSettings.description = gallery.description
            collectionSettings.display_style = gallery.displaystyle
            catalog:withWriteAccessDo('Set LR collection settings', function()
              lrCollection:setRemoteId(uuid)
              lrCollection:setRemoteUrl(gallery.fullurl)
              lrCollection:setCollectionSettings(collectionSettings)
            end)
            createCount = createCount + 1
          else
            log_trace(string.format("SYNC ERROR: PhotoDeck gallery %s '%s' NOT found in Lightroom, and failed to create Published Collection.", uuid, collectionName))
            errorsCount = errorsCount + 1
          end
        else
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' NOT found in Lightroom. Creating Published Collection by default.", uuid, collectionName))
          catalog:withWriteAccessDo('Creating Published Collection', function()
            lrCollection = publishService:createPublishedCollection(collectionName, parentLRCollectionSet)
          end)
          if lrCollection then
            local collectionSettings = {}
            collectionSettings.description = gallery.description
            collectionSettings.display_style = gallery.displaystyle
            catalog:withWriteAccessDo('Set LR collection settings', function()
              lrCollection:setRemoteId(uuid)
              lrCollection:setRemoteUrl(gallery.fullurl)
              lrCollection:setCollectionSettings(collectionSettings)
            end)
            createCount = createCount + 1
          else
            log_trace(string.format("SYNC ERROR: PhotoDeck gallery %s '%s' NOT found in Lightroom, and failed to create Published Collection.", uuid, collectionName))
            errorsCount = errorsCount + 1
          end
        end
      end


      -- Recurse in sub galleries
      if photodeckGalleriesByParent[uuid] and lrCollectionSet then
        synchronizeGallery(depth + 1, uuid, lrCollectionSet)
      end
    end
  end

  synchronizeGallery(1, rootGalleryId, publishService)

  if progressScope:isCanceled() then
    log_trace("SYNC: Canceled")
  end
  log_trace(string.format("SYNC: Done, created: %i, deleted: %i, updated: %i, errors: %i", createCount, deleteCount, updateCount, errorsCount))
  PhotoDeckAPI.canSynchronize = true
  return { created = createCount, deleted = deleteCount, updated = updateCount, errors = errorsCount }
end

-- getPhoto returns a photo with remote ID uuid, or nil if it does not exist
function PhotoDeckAPI.getPhoto(photoId)
  log_trace(string.format('PhotoDeckAPI.getPhoto("%s")', photoId))
  local url = '/medias/' .. photoId .. '.xml'
  local onerror = {}
  onerror["404"] = function() return nil end
  local response, error_msg = PhotoDeckAPI.request('GET', url, nil, onerror)
  local result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.media)
  --log_trace('PhotoDeckAPI.getPhoto: ' .. printTable(result))
  return result, error_msg
end

function PhotoDeckAPI.photosInGallery(urlname, galleryId)
  log_trace(string.format('PhotoDeckAPI.photosInGallery("%s", "%s")', urlname, galleryId))
  local url = '/websites/' .. urlname .. '/galleries/' .. galleryId .. '.xml'
  local response, error_msg = PhotoDeckAPI.request('GET', url, { view = 'details_with_medias' })
  local medias = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.mediasInGallery)

  if not medias and not error_msg then
    error_msg = LOC("$$$/PhotoDeck/API/Gallery/ErrorGettingPhotos=Couldn't get photos in gallery")
  end

  if not error_msg then
    -- turn it into a set for ease of testing inclusion
    local mediaSet = {}
    if medias then
      for _, v in pairs(medias) do
        mediaSet[v] = v
      end
    end
    --log_trace("PhotoDeckAPI.photosInGallery: " .. printTable(mediaSet))
    return mediaSet
  else
    return nil, error_msg
  end
end

function PhotoDeckAPI.subGalleriesInGallery(urlname, galleryId, matchingName, ignore_not_found)
  log_trace(string.format('PhotoDeckAPI.subGalleriesInGallery("%s", "%s", "%s")', urlname, galleryId, matchingName))
  local url = '/websites/' .. urlname .. '/galleries/' .. galleryId .. '/subgalleries.xml'
  local onerror = {}
  if ignore_not_found then
    onerror["404"] = function() return 'NOT_FOUND' end
  end

  local galleries
  local subgalleries = {}
  local response
  local error_msg = nil
  local page = 0
  local totalPages = 1
  local params = { page = page, per_page = 100 }
  if matchingName then
    params['filter[name]'] = matchingName
  end
  while not error_msg and page < totalPages do
    page = page + 1
    params.page = page
    response, error_msg = PhotoDeckAPI.request('GET', url, params, onerror)
    if ignore_not_found and response == 'NOT_FOUND' then
      return nil
    end
    galleries = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.subGalleriesInGallery)
    --log_trace("PhotoDeckAPI.subGalleriesInGallery " .. tostring(page) .. "/" .. tostring(totalPages) .. ": " .. printTable(galleries))

    if not galleries and not error_msg then
      error_msg = LOC("$$$/PhotoDeck/API/Gallery/ErrorGettingSubgalleries=Couldn't get sub galleries in gallery")
    end

    if not error_msg then
      if galleries and galleries[galleryId] and galleries[galleryId].totalpages and galleries[galleryId].totalpages ~= "" then
        totalPages = tonumber(galleries[galleryId].totalpages)
      end
      -- keep only galleries with parent_uuid matching us
      if galleries then
        for uuid, gallery in pairs(galleries) do
          if gallery.parentuuid == galleryId then
            subgalleries[uuid] = gallery
          end
        end
      end
    end
  end
  if not error_msg then
    --log_trace("PhotoDeckAPI.subGalleriesInGallery: " .. printTable(subgalleries))
    return subgalleries
  else
    return nil, error_msg
  end
end

local function buildPhotoInfoFromLrPhoto(photo, updating)
  local photoInfo = {}
  local title = photo:getFormattedMetadata("headline")
  if not title or title == "" then
    title = photo:getFormattedMetadata("title")
  end
  photoInfo['media[title]'] = title
  photoInfo['media[description]'] = photo:getFormattedMetadata("caption")
  photoInfo['media[keywords]'] = photo:getFormattedMetadata("keywordTagsForExport")
  photoInfo['media[location]'] = photo:getFormattedMetadata("location")
  photoInfo['media[city]'] = photo:getFormattedMetadata("city")
  photoInfo['media[state]'] = photo:getFormattedMetadata("stateProvince")
  photoInfo['media[country]'] = photo:getFormattedMetadata("country")
  photoInfo['media[author]'] = photo:getFormattedMetadata("creator")
  photoInfo['media[copyright]'] = photo:getFormattedMetadata("copyright")
  local artist_rating = photo:getFormattedMetadata("rating")
  if updating and not artist_rating then
    photoInfo['media[delete_artist_rating]'] = 1
  else
    photoInfo['media[artist_rating]'] = artist_rating
  end
  return photoInfo
end

local function buildFileUploadParams(contentPath, photo)
  local content = {}
  local upload_location_requested = false
  local file_size = 0
  local mime_type = 'image/jpeg'
  if canRequestUploadLocation then
    local file_attrs = LrFileUtils.fileAttributes(contentPath)
    file_size = file_attrs.fileSize
    table.insert(content, { name = 'media[content][upload_location]', value = 'REQUEST' })
    table.insert(content, { name = 'media[content][file_name]', value = LrPathUtils.leafName(contentPath) })
    table.insert(content, { name = 'media[content][file_size]', value = file_size })
    table.insert(content, { name = 'media[content][mime_type]', value = mime_type })
    upload_location_requested = true
  else
    table.insert(content, { name = 'media[content]', filePath = contentPath, fileName = LrPathUtils.leafName(contentPath), contentType = mime_type })
  end
  return content, upload_location_requested, file_size, mime_type
end

local function handleIndirectUpload(contentPath, urlname, media, file_size, mime_type)
  local error_msg = nil
  local content = {}
  if media.uploadurl and media.uploadurl ~= "" then
    for k,v in pairs(media.uploadparams) do
      table.insert(content, { name = k, value = v })
    end
    table.insert(content, { name = media.uploadfileparam, filePath = contentPath, fileName = media.filename, contentType = mime_type })
    --log_trace('PhotoDeckAPI.handleIndirectUpload: ' .. printTable(content))
    local seq = string.format("%5i", math.random(99999))
    log_trace(string.format(' %s -> %s[multipart] %s', seq, 'POST', media.uploadurl))
    local result, resp_headers
    result, resp_headers = LrHttp.postMultipart(media.uploadurl, content)
    local status_code = "999"
    if resp_headers.status then
      status_code = tostring(resp_headers.status)
      if status_code == "0" and result == "" then
        status_code = "999"
        result = nil
      end
    end
    if status_code >= "200" and status_code <= "299" then
      log_trace(string.format(' %s <- %s', seq, status_code))
      return PhotoDeckAPI.updatePhoto(media.uuid, urlname, {
        contentUploadLocation = media.uploadlocation,
        contentFileName = media.filename,
        contentFileSize = file_size,
        contentMimeType = mime_type }, false)
    end
    if status_code == "999" then
      error_msg = LOC("$$$/PhotoDeck/API/NoResponse=No response from network")
    else
      error_msg = LOC("$$$/PhotoDeck/API/HTTPError=HTTP error ^1", status_code)
    end
    if result then
      log_error(string.format(' %s <- %s: %s %s\n%s', seq, status_code, error_msg, printTable(resp_headers), result))
    else
      log_error(string.format(' %s <- %s: %s %s', seq, status_code, error_msg, printTable(resp_headers)))
    end

  else
    error_msg = LOC("$$$/PhotoDeck/API/Media/UploadURLMissing=Upload URL missing")
    canRequestUploadLocation = false
  end

  return media, error_msg
end

function PhotoDeckAPI.uploadPhoto(urlname, attributes)
  log_trace(string.format('PhotoDeckAPI.uploadPhoto("%s", %s)', urlname, printTable(attributes)))
  local url = '/medias.xml'
  local content = {}
  local upload_location_requested = false
  local file_size
  local mime_type
  if attributes.contentPath then
    content, upload_location_requested, file_size, mime_type = buildFileUploadParams(attributes.contentPath, attributes.lrPhoto)
  end
  if attributes.publishToGallery then
    table.insert(content, { name = 'media[publish_to_galleries]', value = attributes.publishToGallery })
  end
  if attributes.lrPhoto then
    local attributesFromLrPhoto = buildPhotoInfoFromLrPhoto(attributes.lrPhoto)
    for k,v in pairs(attributesFromLrPhoto) do
      table.insert(content, { name = k, value = v })
    end
  end
  --log_trace('PhotoDeckAPI.uploadPhoto: ' .. printTable(content))
  local response, error_msg = PhotoDeckAPI.requestMultiPart('POST', url, content)
  local media = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.media)
  if not media and not error_msg then
    error_msg = LOC("$$$/PhotoDeck/API/Media/UploadFailed=Upload failed")
  end
  --log_trace('PhotoDeckAPI.uploadPhoto: ' .. printTable(media))

  if media and not error_msg and upload_location_requested then
    media, error_msg = handleIndirectUpload(attributes.contentPath, urlname, media, file_size, mime_type)
    if error_msg and not canRequestUploadLocation then
      return PhotoDeckAPI.uploadPhoto(urlname, attributes) -- retry, indirect upload not available
    end
  end

  ratelimit_delay_next(0.334) -- 3 uploads/sec max

  return media, error_msg, (error_msg and PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.uploadStopWithError) == "true")
end

function PhotoDeckAPI.updatePhoto(photoId, urlname, attributes, handleNotFound)
  log_trace(string.format('PhotoDeckAPI.updatePhoto("%s", "%s", %s)', photoId, urlname, printTable(attributes)))
  local url = '/medias/' .. photoId .. '.xml'
  local onerror = {}
  if handleNotFound then
    onerror["404"] = function() return nil, 'Not found' end
  end
  local content = {}
  local upload_location_requested = false
  local file_size
  local mime_type
  if attributes.contentPath then
    content, upload_location_requested, file_size, mime_type = buildFileUploadParams(attributes.contentPath, attributes.lrPhoto)
  end
  if attributes.contentUploadLocation then
    table.insert(content, { name = 'media[content][upload_location]', value = attributes.contentUploadLocation })
    table.insert(content, { name = 'media[content][file_name]', value = attributes.contentFileName })
    table.insert(content, { name = 'media[content][file_size]', value = attributes.contentFileSize })
    table.insert(content, { name = 'media[content][mime_type]', value = attributes.contentMimeType })
  end
  if attributes.publishToGallery then
    table.insert(content, { name = 'media[publish_to_galleries]', value = attributes.publishToGallery })
  end
  if attributes.lrPhoto then
    local attributesFromLrPhoto = buildPhotoInfoFromLrPhoto(attributes.lrPhoto, true)
    for k,v in pairs(attributesFromLrPhoto) do
      table.insert(content, { name = k, value = v })
    end
  end
  --log_trace('PhotoDeckAPI.updatePhoto: ' .. printTable(content))
  local response, error_msg = PhotoDeckAPI.requestMultiPart('PUT', url, content, onerror)
  if handleNotFound and error_msg == 'Not found' then
    return { notfound = true }, error_msg
  end

  local media = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.media)
  if not media and not error_msg then
    error_msg = LOC("$$$/PhotoDeck/API/Media/UpdateFailed=Update failed")
  end
  --log_trace('PhotoDeckAPI.updatePhoto: ' .. printTable(media))

  if media and not error_msg and upload_location_requested then
    media, error_msg = handleIndirectUpload(attributes.contentPath, urlname, media, file_size, mime_type)
    if error_msg and not canRequestUploadLocation then
      return PhotoDeckAPI.updatePhoto(photoId, urlname, attributes, handleNotFound) -- retry, indirect upload not available
    end
  end

  return media, error_msg, (error_msg and PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.uploadStopWithError) == "true")
end

function PhotoDeckAPI.deletePhoto(photoId)
  log_trace(string.format('PhotoDeckAPI.deletePhoto("%s")', photoId))
  local onerror = {}
  onerror["404"] = function() return nil end
  local response, error_msg = PhotoDeckAPI.request('DELETE', '/medias/' .. photoId .. '.xml', nil, onerror)
  --log_trace('PhotoDeckAPI.deletePhoto: ' .. response)
  return response, error_msg
end

function PhotoDeckAPI.deletePhotos(photoIds)
  log_trace(string.format('PhotoDeckAPI.deletePhotos(<photo ids)'))
  local url = '/medias/batch_update.xml'
  local content = { { name = 'medias[on]', value = 'medias' },
                    { name = 'medias[medias]', value = table.concat(photoIds, ',') },
                    { name = 'medias[delete]', value = '1' } }
  local response, error_msg = PhotoDeckAPI.requestMultiPart('PUT', url, content)
  --log_trace('PhotoDeckAPI.deletePhotos: ' .. response)
  return response, error_msg
end

function PhotoDeckAPI.unpublishPhoto(photoId, galleryId)
  log_trace(string.format('PhotoDeckAPI.unpublishPhoto("%s", "%s")', photoId, galleryId))
  local url = '/medias/' .. photoId .. '.xml'
  local content = { { name = 'media[unpublish_from_galleries]', value = galleryId } }
  local onerror = {}
  onerror["404"] = function() return nil end
  local response, error_msg = PhotoDeckAPI.requestMultiPart('PUT', url, content, onerror)
  --log_trace('PhotoDeckAPI.unpublishPhoto: ' .. response)
  return response, error_msg
end

function PhotoDeckAPI.unpublishPhotos(photoIds, galleryId)
  log_trace(string.format('PhotoDeckAPI.unpublishPhotos(<photo ids>, "%s")', galleryId))
  local url = '/medias/batch_update.xml'
  local content = { { name = 'medias[on]', value = 'medias' },
                    { name = 'medias[medias]', value = table.concat(photoIds, ',') },
                    { name = 'medias[unpublish_from_galleries]', value = galleryId } }
  local response, error_msg = PhotoDeckAPI.requestMultiPart('PUT', url, content)
  --log_trace('PhotoDeckAPI.unpublishPhotos: ' .. response)
  return response, error_msg
end

function PhotoDeckAPI.galleryDisplayStyles(urlname)
  log_trace(string.format('PhotoDeckAPI.galleryDisplayStyles("%s")', urlname))
  local cacheKey = 'gallery_display_styles/' .. urlname
  local result = PhotoDeckAPICache[cacheKey]
  local response, error_msg = nil
  if not result then
    local url = '/websites/' .. urlname .. '/gallery_display_styles.xml'
    response, error_msg = PhotoDeckAPI.request('GET', url, { view = 'details' })
    result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.galleryDisplayStyles)
    if not error_msg then
      local styles_count = 0
      if result then
        for _ in pairs(result) do styles_count = styles_count + 1 end
      end
      if styles_count == 0 then
        error_msg = LOC("$$$/PhotoDeck/API/GalleryDisplayStyles/Empty=Couldn't get list of gallery display styles")
      end
    end
    if not error_msg then
      PhotoDeckAPICache[cacheKey] = result
    end
    --log_trace('PhotoDeckAPI.galleryDisplayStyles: ' .. printTable(result))
  end
  return result, error_msg
end

function PhotoDeckAPI.deleteGallery(urlname, galleryId)
  log_trace(string.format('PhotoDeckAPI.deleteGallery("%s", "%s")', urlname, galleryId))
  local url = '/websites/' .. urlname .. '/galleries/' .. galleryId .. '.xml'
  --log_trace(url)
  local response, error_msg = PhotoDeckAPI.request('DELETE', url)
  --log_trace('PhotoDeckAPI.deleteGallery: ' .. response)
  return response, error_msg
end

function PhotoDeckAPI.reorderGallery(urlname, galleryId, mediasIds)
  log_trace(string.format('PhotoDeckAPI.reorderGallery("%s", "%s", %s)', urlname, galleryId, printTable(mediasIds)))

  local seq = ""
  for i, uuid in pairs(mediasIds) do
    if i == 1 then
      seq = uuid
    else
      seq = seq .. "," .. uuid
    end
  end
  log_trace(seq)

  local galleryInfo = {}
  galleryInfo['gallery[content_order]'] = 'manual-last'
  galleryInfo['gallery[medias_order]'] = seq

  local response, error_msg = PhotoDeckAPI.request('PUT', '/websites/' .. urlname .. '/galleries/' .. galleryId .. '.xml', galleryInfo)

  return response, error_msg
end

-- Done
return PhotoDeckAPI
