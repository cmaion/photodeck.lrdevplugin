local LrDate = import 'LrDate'
local LrDigest = import 'LrDigest'
local LrFileUtils = import 'LrFileUtils'
local LrHttp = import 'LrHttp'
local LrStringUtils = import 'LrStringUtils'
local LrXml = import 'LrXml'
local PhotoDeckUtils = require 'PhotoDeckUtils'
local PhotoDeckAPIXSLT = require 'PhotoDeckAPIXSLT'

local logger = import 'LrLogger'( 'PhotoDeckPublishLightroomPlugin' )
logger:enable('logfile')

local urlprefix = 'http://api.photodeck.com'
local isTable = PhotoDeckUtils.isTable
local printTable = PhotoDeckUtils.printTable

local PhotoDeckAPI = {}
local PhotoDeckAPICache = {}

-- sign API request according to docs at
-- http://www.photodeck.com/developers/get-started/
local function sign(method, uri, querystring)
  querystring = querystring or ''
  local cocoatime = LrDate.currentTime()
  -- cocoatime = cocoatime - (cocoatime % 600)
  -- Fri, 25 Jun 2010 12:39:15 +0200
  local timestamp = LrDate.timeToUserFormat(cocoatime, "%b, %d %Y %H:%M:%S -0000", true)

  local request = string.format('%s\n%s\n%s\n%s\n%s\n', method, uri,
                                querystring, PhotoDeckAPI.secret, timestamp)
  local signature = PhotoDeckAPI.key .. ':' .. LrDigest.SHA1.digest(request)
  -- logger:trace(timestamp)
  -- logger:trace(signature)
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
    local authorization = 'Basic ' .. LrStringUtils.encodeBase64(PhotoDeckAPI.username ..
                                                             ':' .. PhotoDeckAPI.password)
    table.insert(headers, { field = 'Authorization',  value=authorization })
  end
  return headers
end

-- extra chars from http://tools.ietf.org/html/rfc3986#section-2.2
local function urlencode (s)
  s = string.gsub(s, "([][:/?#@!#'()*,;&=+%c])", function (c)
         return string.format("%%%02X", string.byte(c))
      end)
  s = string.gsub(s, " ", "+")
  return s
end

-- convert lua table to url encoded data
-- from http://www.lua.org/pil/20.3.html
local function table_to_querystring(data)
  assert(PhotoDeckUtils.isTable(data))

  local s = ""
  for k,v in pairs(data) do
    s = s .. "&" .. urlencode(k) .. "=" .. urlencode(v)
  end
  return string.sub(s, 2)     -- remove first `&'
end

local function handle_response(seq, response, resp_headers, onerror)
  local status = PhotoDeckUtils.filter(resp_headers, function(v) return isTable(v) and v.field == 'Status' end)[1]
  local request_id = PhotoDeckUtils.filter(resp_headers, function(v) return isTable(v) and v.field == 'X-Request-Id' end)[1]
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

  if status_code >= "400" then
    if status then
      -- Get error from Status header
      error_msg = status.value
    else
      -- Generic HTTP error
      if status_code == "999" then
        error_msg = "Unknwon error"
      else
        error_msg = "HTTP error " .. status_code
      end
    end

    if not response and status_code == "999" then
      error_msg = "No response from network"
    end

    local error_msg_from_xml = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.error)
    if error_msg_from_xml and error_msg_from_xml ~= "" then
      -- We got an error from the API, use that error message instead
      error_msg = error_msg_from_xml
    end

    if onerror and onerror[status_code] then
      logger:trace(string.format(' %s <- %s [%s] (handled by onerror)', seq, status_code, request_id))
      return onerror[status_code]()
    end

    --logger:error("Bad response: " .. error_msg .. " => " .. (response or "(no response)"))
    --if resp_headers then
    --  logger:error(PhotoDeckUtils.printLrTable(resp_headers))
    --end
    logger:error(string.format(' %s <- %s [%s]: %s', seq, status_code, request_id, error_msg))
  else
    logger:trace(string.format(' %s <- %s [%s]', seq, status_code, request_id))
  end

  return response, error_msg
end

-- make HTTP GET request to PhotoDeck API
-- must be called within an LrTask
function PhotoDeckAPI.request(method, uri, data, onerror)
  local querystring = ''
  local body = ''
  if data then
    if method == 'GET' then
      querystring = table_to_querystring(data)
    else
      body = table_to_querystring(data)
    end
  end

  -- set up authorisation headers
  local headers = auth_headers(method, uri, querystring)

  -- build full url
  local fullurl = urlprefix .. uri
  if querystring and querystring ~= '' then
    fullurl = fullurl .. '?' .. querystring
  end

  -- call API
  local result, resp_headers
  local seq = string.format("%5i", math.random(99999))
  if method == 'GET' then
    logger:trace(string.format(' %s -> %s %s', seq, method, fullurl))
    result, resp_headers = LrHttp.get(fullurl, headers)
  else
    -- override default Content-Type!
    logger:trace(string.format(' %s -> %s %s\n%s', seq, method, fullurl, body))
    table.insert(headers, { field = 'Content-Type',  value = 'application/x-www-form-urlencoded'})
    result, resp_headers = LrHttp.post(fullurl, body, headers, method)
  end

  result, error_msg = handle_response(seq, result, resp_headers, onerror)
  return result, error_msg
end

function PhotoDeckAPI.requestMultiPart(method, uri, content, onerror)
  local seq = string.format("%5i", math.random(99999))
  logger:trace(string.format(' %s -> %s[multipart] %s', seq, method, uri))

  if method ~= "POST" then
    -- LrHttp doesn't implement non-POSTs multipart requests:
    -- POST content but pass the correct method to the PhotoDeck API as a field
    table.insert(content, { name = "_method", value = method })
    method = "POST"
  end

  -- set up authorisation headers
  local headers = auth_headers(method, uri)
  -- build full url
  local fullurl = urlprefix .. uri

  -- call API
  local result, resp_headers
  result, resp_headers = LrHttp.postMultipart(fullurl, content, headers)

  result, error_msg = handle_response(seq, result, resp_headers, onerror)

  return result, error_msg
end

function PhotoDeckAPI.connect(key, secret, username, password)
  PhotoDeckAPI.key = key
  PhotoDeckAPI.secret = secret
  PhotoDeckAPI.username = username
  PhotoDeckAPI.password = password
  PhotoDeckAPI.loggedin = false
end

function PhotoDeckAPI.ping(text)
  logger:trace('PhotoDeckAPI.ping()')
  local t = {}
  if text then
    t = { text = text }
  end
  local response, error_msg = PhotoDeckAPI.request('GET', '/ping.xml', t)
  local result = error_msg
  if not error_msg then
    result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.ping)
    if not result or result == "" then
      result = error_msg
    end
  end
  return result, error_msg
end

function PhotoDeckAPI.whoami()
  logger:trace('PhotoDeckAPI.whoami()')
  local response, error_msg = PhotoDeckAPI.request('GET', '/whoami.xml')
  local result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.whoami)
  -- logger:trace(printTable(result))
  return result, error_msg
end

function PhotoDeckAPI.websites()
  logger:trace('PhotoDeckAPI.websites()')
  local result = PhotoDeckAPICache['websites']
  local response, error_msg = nil
  if not result then
    response, error_msg = PhotoDeckAPI.request('GET', '/websites.xml', { view = 'details' })
    result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.websites)
    if not error_msg then
      websites_count = 0
      if result then
        for _ in pairs(result) do websites_count = websites_count + 1 end
      end
      if websites_count == 0 then
        error_msg = "No websites found"
      end
    end
    if not error_msg then
      PhotoDeckAPICache['websites'] = result
    end
    -- logger:trace(printTable(result))
  end
  return result, error_msg
end

function PhotoDeckAPI.website(urlname)
  local websites, error_msg = PhotoDeckAPI.websites()
  local website = nil
  if not error_msg then
    website = websites[urlname]
    if not website then 
      error_msg = "Website not found"
    end
  end
  return website, error_msg
end

function PhotoDeckAPI.galleries(urlname)
  logger:trace(string.format('PhotoDeckAPI.galleries("%s")', urlname))
  local response, error_msg = PhotoDeckAPI.request('GET', '/websites/' .. urlname .. '/galleries.xml', { view = 'details' })
  local result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.galleries)
  -- logger:trace(printTable(result))
  return result, error_msg
end

function PhotoDeckAPI.gallery(urlname, galleryId)
  logger:trace(string.format('PhotoDeckAPI.gallery("%s", "%s")', urlname, galleryId))
  local onerror = {}
  onerror["404"] = function() return nil end
  local response, error_msg = PhotoDeckAPI.request('GET', '/websites/' .. urlname .. '/galleries/' .. galleryId .. '.xml', { view = 'details' }, onerror)
  local result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.gallery)
  -- logger:trace(printTable(result))
  return result, error_msg
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
  logger:trace(string.format('PhotoDeckAPI.createGallery("%s", "%s", <collectionInfo>)', urlname, parentId))
  local galleryInfo = buildGalleryInfoFromLrCollectionInfo(collectionInfo)
  galleryInfo['gallery[parent]'] = parentId
  local response, error_msg = PhotoDeckAPI.request('POST', '/websites/' .. urlname .. '/galleries.xml', galleryInfo)
  local result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.createGallery)

  if error_msg then
    return result, error_msg
  end

  local gallery, error_msg = PhotoDeckAPI.gallery(urlname, result['uuid'])
  return gallery, error_msg
end

function PhotoDeckAPI.updateGallery(urlname, galleryId, parentId, collectionInfo)
  logger:trace(string.format('PhotoDeckAPI.updateGallery("%s", "%s", "%s", <collectionInfo>)', urlname, galleryId, parentId))
  local galleryInfo = buildGalleryInfoFromLrCollectionInfo(collectionInfo)
  galleryInfo['gallery[parent]'] = parentId
  local response, error_msg = PhotoDeckAPI.request('PUT', '/websites/' .. urlname .. '/galleries/' .. galleryId .. '.xml', galleryInfo)

  if error_msg then
    return nil, error_msg
  end

  local gallery, error_msg = PhotoDeckAPI.gallery(urlname, galleryId)
  return gallery, error_msg
end


function PhotoDeckAPI.createOrUpdateGallery(urlname, collectionInfo)
  logger:trace(string.format('PhotoDeckAPI.createOrUpdateGallery("%s", <collectionInfo>)', urlname))

  local website, error_msg = PhotoDeckAPI.website(urlname)
  if error_msg then
    return nil, error_msg
  end

  local collection = collectionInfo.publishedCollection

  local parentGalleryId = nil
  local parentJustCreated = false

  -- Find PhotoDeck gallery and parent gallery
  local gallery = nil
  local galleryId = collection:getRemoteId()
  if galleryId then
    -- find by remote ID if known
    gallery = PhotoDeckAPI.gallery(urlname, galleryId)

    if gallery then
      -- check LR parent and see if we the PD gallery is still properly connected
      for _, parent in pairs(collectionInfo.parents) do
        if parent.remoteCollectionId == gallery.parentuuid then
          -- ok, found, no need to go back to all parents one by one to reconnect everything
          parentGalleryId = gallery.parentuuid
	  break
        end
      end
    end
  end


  if not gallery or not parentGalleryId then
    -- Find PhotoDeck parent galleries, create if missing and connect them to LightRoom if not already done

    -- Start from the root gallery
    parentGalleryId = website.rootgalleryuuid
    if not parentGalleryId or parentGalleryId == "" then
      return nil, "Couldn't find root gallery"
    end

    -- Now iterate over each parent, starting from the top level
    for _, parent in pairs(collectionInfo.parents) do
      local parentGallery = nil
      local parentId = parent.remoteCollectionId
      if parentId then
        -- find by remote ID if known
        parentGallery = PhotoDeckAPI.gallery(urlname, parentId)
      end
      if not parentGallery and not parentJustCreated then
        -- not found, search by name within subgalleries present in our parent
	-- (unless we have just created this gallery, in which case we assume that it's empty)
        local subgalleries, error_msg = PhotoDeckAPI.subGalleriesInGallery(urlname, parentGalleryId)
        if error_msg then
          return nil, error_msg
        end
        for uuid, subgallery in pairs(subgalleries) do
          if subgallery.name == parent.name then
	    parentGallery, error_msg = PhotoDeckAPI.gallery(urlname, uuid)
	    if error_msg or not parentGallery then
              return nil, error_msg or "Couldn't get subgallery"
            end
	    break
          end
        end
      end
      if not parentGallery then
        -- not found, create
        parentGallery, error_msg = PhotoDeckAPI.createGallery(urlname, parentGalleryId, parent:getCollectionInfoSummary())
        if error_msg then
          return nil, error_msg
        end
	parentJustCreated = true
      else
        parentJustCreated = false
      end
      local parentCollection = collection.catalog:getPublishedCollectionByLocalIdentifier(parent.localCollectionId)
      parentGallery.fullurl = website.homeurl .. "/-/" .. parentGallery.fullurlpath
      if parentCollection and (not parent.remoteCollectionId or parentCollection:getRemoteId() ~= parent.remoteCollectionId or parentCollection:getRemoteUrl() ~= parentGallery.fullurl) then
        --logger:trace('Updating parent remote Id and Url')
        parentCollection.catalog:withWriteAccessDo('Set Parent Remote Id and Url', function()
          parentCollection:setRemoteId(parentGallery.uuid)
          parentCollection:setRemoteUrl(parentGallery.fullurl)
        end)
      end
  
      parentGalleryId = parentGallery.uuid -- our parent gallery is now this one
    end

    -- now search by name within subgalleries present in our parent
    -- (unless we have just created the parent gallery, in which case we assume that it's empty)
    if not parentJustCreated then
      local subgalleries, error_msg = PhotoDeckAPI.subGalleriesInGallery(urlname, parentGalleryId)
      if error_msg then
        return nil, error_msg
      end
      for uuid, subgallery in pairs(subgalleries) do
        if subgallery.name == collectionInfo.name then
  	  gallery, error_msg = PhotoDeckAPI.gallery(urlname, uuid)
          if error_msg or not gallery then
            return nil, error_msg or "Couldn't get subgallery"
          end
  	  break
        end
      end
    end
  end

  if gallery then
    -- PhotoDeck gallery found, update
    gallery, error_msg = PhotoDeckAPI.updateGallery(urlname, gallery.uuid, parentGalleryId, collectionInfo)
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
    --logger:trace('Updating collection remote Id and Url')
    collection.catalog:withWriteAccessDo('Set Remote Id and Url', function()
      collection:setRemoteId(gallery.uuid)
      collection:setRemoteUrl(gallery.fullurl)
    end)
  end
  return gallery
end

-- getPhoto returns a photo with remote ID uuid, or nil if it does not exist
function PhotoDeckAPI.getPhoto(photoId)
  logger:trace(string.format('PhotoDeckAPI.getPhoto("%s")', photoId))
  local url = '/medias/' .. photoId .. '.xml'
  local onerror = {}
  onerror["404"] = function() return nil end
  local response, error_msg = PhotoDeckAPI.request('GET', url, nil, onerror)
  local result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.getPhoto)
  --logger:trace('PhotoDeckAPI.getPhoto: ' .. printTable(result))
  return result, error_msg
end

function PhotoDeckAPI.photosInGallery(urlname, galleryId)
  logger:trace(string.format('PhotoDeckAPI.photosInGallery("%s", "%s")', urlname, galleryId))
  local url = '/websites/' .. urlname .. '/galleries/' .. galleryId .. '.xml'
  local response, error_msg = PhotoDeckAPI.request('GET', url, { view = 'details_with_medias' })
  local medias = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.photosInGallery)
  
  if not medias and not error_msg then
    error_msg = "Couldn't get photos in gallery"
  end

  if not error_msg then
    -- turn it into a set for ease of testing inclusion
    local mediaSet = {}
    if medias then
      for _, v in pairs(medias) do
        mediaSet[v] = v
      end
    end
    --logger:trace("PhotoDeckAPI.photosInGallery: " .. printTable(mediaSet))
    return mediaSet
  else
    return nil, error_msg
  end
end

function PhotoDeckAPI.subGalleriesInGallery(urlname, galleryId)
  logger:trace(string.format('PhotoDeckAPI.subGalleriesInGallery("%s", "%s")', urlname, galleryId))
  local url = '/websites/' .. urlname .. '/galleries/' .. galleryId .. '/subgalleries.xml'

  local galleries
  local subgalleries = {}
  local response
  local error_msg = nil
  local page = 0
  local totalPages = 1
  while not error_msg and page < totalPages do
    page = page + 1
    response, error_msg = PhotoDeckAPI.request('GET', url, { page = page, per_page = 100 })
    galleries = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.subGalleriesInGallery)
    --logger:trace("PhotoDeckAPI.subGalleriesInGallery " .. tostring(page) .. "/" .. tostring(totalPages) .. ": " .. printTable(galleries))
  
    if not galleries and not error_msg then
      error_msg = "Couldn't get subgalleries"
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
    --logger:trace("PhotoDeckAPI.subGalleriesInGallery: " .. printTable(subgalleries))
    return subgalleries
  else
    return nil, error_msg
  end
end

local function buildPhotoInfoFromLrPhoto(photo)
  local photoInfo = {}
  photoInfo['media[title]'] = photo:getFormattedMetadata("title")
  photoInfo['media[description]'] = photo:getFormattedMetadata("caption")
  photoInfo['media[keywords]'] = photo:getFormattedMetadata("keywordTagsForExport")
  photoInfo['media[location]'] = photo:getFormattedMetadata("location")
  photoInfo['media[city]'] = photo:getFormattedMetadata("city")
  photoInfo['media[state]'] = photo:getFormattedMetadata("stateProvince")
  photoInfo['media[country]'] = photo:getFormattedMetadata("country")
  photoInfo['media[author]'] = photo:getFormattedMetadata("creator")
  photoInfo['media[copyright]'] = photo:getFormattedMetadata("copyright")
  return photoInfo
end

function PhotoDeckAPI.uploadPhoto(urlname, attributes)
  logger:trace(string.format('PhotoDeckAPI.uploadPhoto("%s", <attributes>)', urlname))
  local url = '/medias.xml'
  local content = {}
  if attributes.contentPath then
    table.insert(content, { name = 'media[content]', filePath = attributes.contentPath, fileName = PhotoDeckUtils.basename(attributes.contentPath), contentType = 'image/jpeg' })
  end
  if attributes.publishToGallery then
    table.insert(content, { name = 'media[publish_to_galleries]', value = attributes.publishToGallery.uuid })
  end
  if attributes.lrPhoto then
    local attributesFromLrPhoto = buildPhotoInfoFromLrPhoto(attributes.lrPhoto)
    for k,v in pairs(attributesFromLrPhoto) do
      table.insert(content, { name = k, value = v })
    end
  end
  --logger:trace('PhotoDeckAPI.uploadPhoto: ' .. printTable(content))
  local response, error_msg = PhotoDeckAPI.requestMultiPart('POST', url, content)
  local media = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.uploadPhoto)
  if not media and not error_msg then
    error_msg = "Photo upload failed"
  end
  if not error_msg and attributes.publishToGallery then
    local website = PhotoDeckAPI.website(urlname)
    if website then
      media.url = website.homeurl .. '/-/' .. attributes.publishToGallery.fullurlpath .. "/-/medias/" .. media.uuid
    end
  end
  --logger:trace('PhotoDeckAPI.uploadPhoto: ' .. printTable(media))
  return media, error_msg
end

function PhotoDeckAPI.updatePhoto(photoId, urlname, attributes)
  logger:trace(string.format('PhotoDeckAPI.updatePhoto("%s", "%s", <attributes>)', photoId, urlname))
  local url = '/medias/' .. photoId .. '.xml'
  local content = {}
  if attributes.contentPath then
    table.insert(content, { name = 'media[content]', filePath = attributes.contentPath, fileName = PhotoDeckUtils.basename(attributes.contentPath), contentType = 'image/jpeg' })
  end
  if attributes.publishToGallery then
    table.insert(content, { name = 'media[publish_to_galleries]', value = attributes.publishToGallery.uuid })
  end
  if attributes.lrPhoto then
    local attributesFromLrPhoto = buildPhotoInfoFromLrPhoto(attributes.lrPhoto)
    for k,v in pairs(attributesFromLrPhoto) do
      table.insert(content, { name = k, value = v })
    end
  end
  --logger:trace('PhotoDeckAPI.updatePhoto: ' .. printTable(content))
  local response, error_msg = PhotoDeckAPI.requestMultiPart('PUT', url, content)
  local media = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.updatePhoto)
  if not media and not error_msg then
    error_msg = "Photo update failed"
  end
  if not error_msg and attributes.publishToGallery then
    local website = PhotoDeckAPI.website(urlname)
    media.url = website.homeurl .. '/-/' .. attributes.publishToGallery.fullurlpath .. "/-/medias/" .. media.uuid
  end
  --logger:trace('PhotoDeckAPI.updatePhoto: ' .. printTable(media))
  return media, error_msg
end

function PhotoDeckAPI.deletePhoto(photoId)
  logger:trace(string.format('PhotoDeckAPI.deletePhoto("%s")', photoId))
  local onerror = {}
  onerror["404"] = function() return nil end
  local response, error_msg = PhotoDeckAPI.request('DELETE', '/medias/' .. photoId .. '.xml', nil, onerror)
  --logger:trace('PhotoDeckAPI.deletePhoto: ' .. response)
  return response, error_msg
end

function PhotoDeckAPI.unpublishPhoto(photoId, galleryId)
  logger:trace(string.format('PhotoDeckAPI.unpublishPhoto("%s", "%s")', photoId, galleryId))
  local url = '/medias/' .. photoId .. '.xml'
  local content = { { name = 'media[unpublish_from_galleries]', value = galleryId } }
  local onerror = {}
  onerror["404"] = function() return nil end
  local response, error_msg = PhotoDeckAPI.requestMultiPart('PUT', url, content, onerror)
  --logger:trace('PhotoDeckAPI.unpublishPhoto: ' .. response)
  return response, error_msg
end

function PhotoDeckAPI.galleryDisplayStyles(urlname)
  logger:trace(string.format('PhotoDeckAPI.galleryDisplayStyles("%s")', urlname))
  local result = PhotoDeckAPICache['gallery_display_styles/' .. urlname]
  local response, error_msg = nil
  if not result then
    local url = '/websites/' .. urlname .. '/gallery_display_styles.xml'
    response, error_msg = PhotoDeckAPI.request('GET', url, { view = 'details' })
    result = PhotoDeckAPIXSLT.transform(response, PhotoDeckAPIXSLT.galleryDisplayStyles)
    if not error_msg then
      styles_count = 0
      if result then
        for _ in pairs(result) do styles_count = styles_count + 1 end
      end
      if styles_count == 0 then
        error_msg = "Couldn't get list of gallery display styles"
      end
    end
    if not error_msg then
      PhotoDeckAPICache['gallery_display_styles/' .. urlname] = result
    end
    --logger:trace('PhotoDeckAPI.galleryDisplayStyles: ' .. printTable(result))
  end
  return result, error_msg
end

function PhotoDeckAPI.deleteGallery(urlname, galleryId)
  logger:trace(string.format('PhotoDeckAPI.deleteGallery("%s", "%s")', urlname, galleryId))
  local url = '/websites/' .. urlname .. '/galleries/' .. galleryId .. '.xml'
  --logger:trace(url)
  local response, error_msg = PhotoDeckAPI.request('DELETE', url)
  --logger:trace('PhotoDeckAPI.deleteGallery: ' .. response)
  return response, error_msg
end

return PhotoDeckAPI
