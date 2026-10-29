local LrDate = import("LrDate")
local LrFileUtils = import("LrFileUtils")
local LrHttp = import("LrHttp")
local LrPathUtils = import("LrPathUtils")
local LrTasks = import("LrTasks")
local PhotoDeckHttp = require("PhotoDeckHttp")
local JSON = require("PhotoDeckJSON")
local OAuth = require("PhotoDeckOAuth")

local logger = import("LrLogger")("PhotoDeckPublishLightroomPlugin")
logger:enable("logfile")
local log_trace = logger:quick("trace")

local PhotoDeckAPI_BASEURL = "https://api.photodeck.com"
local PhotoDeckMY_BASEURL = "https://my.photodeck.com"

local PhotoDeckOAUTH_CLIENT_ID = ""

local PhotoDeckAPI = {
  hasDistributionClientId = PhotoDeckOAUTH_CLIENT_ID ~= "",
  loggedin = false,
  canSynchronize = true,
}

local contexts = {}
-- Lightroom publish jobs can run concurrently for different accounts. Keep
-- their credentials, resource caches and failed storage locations task-local.
local taskContexts = setmetatable({}, { __mode = "k" })
local defaultContext = { cache = { values = {}, pending = {} }, failedUploadLocations = {} }
local function currentContext()
  return taskContexts[coroutine.running()] or defaultContext
end
local http = PhotoDeckHttp.new(function() return currentContext().connection end, PhotoDeckAPI_BASEURL)

-- All calls must run in an LrTask. The third result carries status and Link headers.
function PhotoDeckAPI.request(...)
  local result, message, info = http.request(...)
  PhotoDeckAPI.loggedin = PhotoDeckAPI.isConnected()
  return result, message, info
end

local function invalidResponse()
  return LOC("$$$/PhotoDeck/API/InvalidResponse=Invalid response from PhotoDeck")
end

local function cached(key, load)
  local context = currentContext()
  local cache = context.cache
  if cache.values[key] then
    return cache.values[key]
  end
  local pending = cache.pending[key]
  if pending then
    while pending.loading do
      LrTasks.sleep(0.1)
    end
    return pending.result, pending.error
  end
  pending = { loading = true }
  cache.pending[key] = pending
  local ok, result, errorMessage = LrTasks.pcall(function()
    -- LrTasks.pcall may use a child coroutine to allow protected yielding.
    local task = coroutine.running()
    if task then
      taskContexts[task] = context
    end
    return load()
  end)
  if not ok then
    result, errorMessage = nil, invalidResponse()
  end
  pending.result, pending.error, pending.loading = result, errorMessage, false
  cache.pending[key] = nil
  if result and not errorMessage then
    cache.values[key] = result
  end
  return result, errorMessage
end

local function requestObject(method, url, params, onerror)
  local result, errorMessage, info = PhotoDeckAPI.request(method, url, params, onerror)
  if (result and not JSON.isObject(result)) or (info and info.status == 204) then
    return nil, invalidResponse(), info
  end
  return result, errorMessage, info
end

local function requestList(url, params, key, onerror, inclusion)
  local result, visited = {}, {}
  while url do
    local target = http.target(url)
    if not target or visited[target] then
      return nil, invalidResponse()
    end
    visited[target] = true
    local page, errorMessage, info = PhotoDeckAPI.request("GET", url, params, onerror)
    if errorMessage or not page then
      return nil, errorMessage
    end
    local items = inclusion and page[inclusion] or page
    if not JSON.isArray(items) then
      return nil, invalidResponse()
    end
    for _, item in ipairs(items) do
      if inclusion then
        if type(item) ~= "string" then
          return nil, invalidResponse()
        end
        result[item] = item
      elseif not JSON.isObject(item) or type(item[key or "uuid"]) ~= "string" then
        return nil, invalidResponse()
      else
        result[item[key or "uuid"]] = item
      end
    end
    url, params = info.links.next, nil
  end
  return result
end

local function galleryPath(urlname, galleryId)
  return "/websites/" .. PhotoDeckHttp.encode(urlname) .. "/galleries" .. (galleryId and "/" .. PhotoDeckHttp.encode(galleryId) or "") .. ".json"
end

local function galleryURL(website, gallery)
  return website.home_url:gsub("/+$", "") .. "/-/" .. gallery.full_url_path:gsub("^/+", "")
end

function PhotoDeckAPI.connect(settings)
  local clientId = PhotoDeckAPI.hasDistributionClientId and PhotoDeckOAUTH_CLIENT_ID or settings.oauthClientId
  local selected = OAuth.connection(clientId, settings.oauthConnectionId, PhotoDeckMY_BASEURL, PhotoDeckAPI_BASEURL)
  local context = selected and contexts[selected]
  if not context then
    context = { connection = selected, cache = { values = {}, pending = {} }, failedUploadLocations = {} }
    if selected then
      contexts[selected] = context
    end
  end
  local task = coroutine.running()
  if task then
    taskContexts[task] = context
  else
    defaultContext = context
  end
  PhotoDeckAPI.loggedin = PhotoDeckAPI.isConnected()
  return selected
end

function PhotoDeckAPI.isConnected()
  local selected = currentContext().connection
  return selected and selected.isConnected() or false
end

function PhotoDeckAPI.authorize(settings, cancel)
  if not PhotoDeckAPI.hasDistributionClientId and (not settings.oauthClientId or settings.oauthClientId == "") then
    return nil, LOC("$$$/PhotoDeck/OAuth/ClientIdRequired=Configure the PhotoDeck OAuth client ID first")
  end
  if not settings.oauthConnectionId or settings.oauthConnectionId == "" then
    local data, message = require("PhotoDeckPKCE").generate()
    if not data then
      return nil, message
    end
    settings.oauthConnectionId = data.state
  end
  local selected = PhotoDeckAPI.connect(settings)
  if not selected then
    return nil, LOC("$$$/PhotoDeck/OAuth/ClientIdRequired=Configure the PhotoDeck OAuth client ID first")
  end
  local result, message = selected.authorize(cancel)
  contexts[selected].cache = { values = {}, pending = {} }
  PhotoDeckAPI.loggedin = selected.isConnected()
  return result, message
end

function PhotoDeckAPI.disconnect(settings)
  local selected = PhotoDeckAPI.connect(settings)
  local result, error_msg = true, nil
  if selected then
    result, error_msg = selected.disconnect()
  end
  PhotoDeckAPI.loggedin = false
  currentContext().cache = { values = {}, pending = {} }
  return result, error_msg
end

function PhotoDeckAPI.whoami()
  log_trace("PhotoDeckAPI.whoami()")
  local result, error_msg = requestObject("GET", "/whoami.json")
  if not result then
    PhotoDeckAPI.loggedin = false
  elseif type(result.email) ~= "string" or result.email == "" then
    return nil, invalidResponse()
  end
  return result, error_msg
end

function PhotoDeckAPI.websites()
  log_trace("PhotoDeckAPI.websites()")
  return cached("websites", function()
    local result, error_msg = requestList("/websites.json", { per_page = 100 }, "urlname")
    if result and not next(result) then
      return nil, LOC("$$$/PhotoDeck/API/Websites/NotFound=No websites found")
    end
    return result, error_msg
  end)
end

function PhotoDeckAPI.website(urlname, details)
  if details or currentContext().cache.values["website/" .. urlname] then
    return cached("website/" .. urlname, function()
      local result, message = requestObject("GET", "/websites/" .. PhotoDeckHttp.encode(urlname) .. ".json")
      if result and (type(result.uuid) ~= "string" or type(result.home_url) ~= "string") then
        return nil, invalidResponse()
      end
      return result, message
    end)
  end
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

function PhotoDeckAPI.artists()
  log_trace("PhotoDeckAPI.artists()")
  return cached("artists", function()
    local result, error_msg = requestList("/artists.json", { per_page = 100 })
    if result and not next(result) then
      return nil, LOC("$$$/PhotoDeck/API/Artists/NotFound=No media library found")
    end
    return result, error_msg
  end)
end

function PhotoDeckAPI.watermarks(artist)
  log_trace(string.format('PhotoDeckAPI.watermarks("%s")', artist or "?"))
  return cached("watermarks/" .. (artist or "?"), function()
    local result, error_msg = {}, nil
    result[""] = {
      name = LOC("$$$/PhotoDeck/API/Watermarks/Default=PhotoDeck default"),
    }
    result["none"] = {
      name = LOC("$$$/PhotoDeck/API/Watermarks/None=None"),
    }
    if artist and artist ~= "" then
      local watermarks
      watermarks, error_msg = requestList("/artists/" .. PhotoDeckHttp.encode(artist) .. "/watermarks.json", { per_page = 1000 })
      if watermarks then
        for uuid, watermark in pairs(watermarks) do
          result[uuid] = watermark
          if watermark.file_name and watermark.file_name ~= "" then
            result[uuid]["name"] = LOC("$$$/PhotoDeck/API/Watermarks/File=File:") .. " <" .. watermark.file_name .. ">"
          else
            result[uuid]["name"] = LOC("$$$/PhotoDeck/API/Watermarks/Text=Text:") .. " " .. (watermark.text or "")
          end
        end
      end
    end
    return result, error_msg
  end)
end

function PhotoDeckAPI.galleries(urlname)
  log_trace(string.format('PhotoDeckAPI.galleries("%s")', urlname))
  return requestList(galleryPath(urlname), { per_page = 1000 })
end

function PhotoDeckAPI.gallery(urlname, galleryId, ignore_not_found)
  log_trace(string.format('PhotoDeckAPI.gallery("%s", "%s")', urlname, galleryId))
  local onerror = {}
  if ignore_not_found then
    onerror["404"] = function()
      return nil
    end
  end
  return requestObject("GET", galleryPath(urlname, galleryId), nil, onerror)
end

function PhotoDeckAPI.openGalleryInBackend(galleryId)
  log_trace(string.format('PhotoDeckAPI.openGalleryInBackend("%s")', galleryId))
  LrHttp.openUrlInBrowser(PhotoDeckMY_BASEURL .. "/medias/manage?gallery_uuid=" .. galleryId)
end

local function buildGalleryParams(collectionInfo, includeSettings)
  local params = { name = collectionInfo.name }
  local collectionSettings = collectionInfo.collectionSettings
  if collectionSettings and includeSettings then
    params.description = collectionSettings.description
    params.gallery_display_style_uuid = collectionSettings.display_style
  end
  return params
end

function PhotoDeckAPI.createGallery(urlname, parentId, collectionInfo)
  log_trace(string.format('PhotoDeckAPI.createGallery("%s", "%s", <collectionInfo>)', urlname, parentId))
  local params = buildGalleryParams(collectionInfo, true)
  params.content_order = "manual-last"
  params.parent_uuid = parentId
  return requestObject("POST", galleryPath(urlname), { gallery = params })
end

function PhotoDeckAPI.updateGallery(urlname, galleryId, parentId, collectionInfo, updateSettings)
  log_trace(string.format('PhotoDeckAPI.updateGallery("%s", "%s", "%s", <collectionInfo>)', urlname, galleryId, parentId))
  local params = buildGalleryParams(collectionInfo, updateSettings)
  params.parent_uuid = parentId
  return requestObject("PUT", galleryPath(urlname, galleryId), { gallery = params })
end

function PhotoDeckAPI.createOrUpdateGallery(urlname, collectionInfo, updateSettings)
  log_trace(string.format('PhotoDeckAPI.createOrUpdateGallery("%s", <collectionInfo>)', urlname))

  local website, error_msg = PhotoDeckAPI.website(urlname, true)
  if error_msg or not website then
    return nil, error_msg
  end

  local collection = collectionInfo.publishedCollection

  -- Find PhotoDeck gallery (if PhotoDeck's ID is known)
  local gallery = nil
  local galleryId = collection:getRemoteId()
  if galleryId and galleryId ~= "" then
    -- find by remote ID if known
    gallery, error_msg = PhotoDeckAPI.gallery(urlname, galleryId, true)
    if error_msg then
      return nil, error_msg
    end
  end

  -- Find PhotoDeck parent gallery ID
  local parentGalleryId = nil
  local parentGalleryMissingId = nil
  local parentJustCreated = false

  parentGalleryId = website.root_gallery_uuid
  if not parentGalleryId or parentGalleryId == "" then
    return nil, LOC("$$$/PhotoDeck/API/Galleries/RootNotFound=Couldn't find PhotoDeck root gallery")
  end
  for _, parent in ipairs(collectionInfo.parents) do
    parentGalleryId = parent.remoteCollectionId
  end
  if gallery and parentGalleryId ~= gallery.parent_uuid then
    -- Resolve the parent hierarchy when the remote parent differs.
    parentGalleryId = nil
  end

  local lookupIntoParents = not gallery or not parentGalleryId
  local iterations = 0
  while lookupIntoParents do
    iterations = iterations + 1
    if iterations > 2 then -- Bound parent-recovery attempts.
      return nil, error_msg or LOC("$$$/PhotoDeck/API/Gallery/SubGalleryNotFound=Couldn't get subgallery")
    end
    lookupIntoParents = false
    parentJustCreated = false

    if not parentGalleryId then
      -- PhotoDeck parent gallery ID unknown: find all parent galleries, create
      -- if missing and connect them to Lightroom if not already done

      -- Start from the root gallery
      parentGalleryId = website.root_gallery_uuid

      -- Now iterate over each parent, starting from the top level
      for _, parent in ipairs(collectionInfo.parents) do
        local parentGallery = nil
        local parentId = parent.remoteCollectionId
        if parentId and parentId ~= "" and parentId ~= parentGalleryMissingId then
          -- find by remote ID if known
          parentGallery, error_msg = PhotoDeckAPI.gallery(urlname, parentId, true)
          if error_msg then
            return nil, error_msg
          end
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
        parentGallery.full_url = galleryURL(website, parentGallery)
        if
          parentCollection
          and (
            not parent.remoteCollectionId
            or parentCollection:getRemoteId() ~= parentGallery.uuid
            or parentCollection:getRemoteUrl() ~= parentGallery.full_url
          )
        then
          parentCollection.catalog:withWriteAccessDo("Set Parent Remote Id and Url", function()
            parentCollection:setRemoteId(parentGallery.uuid)
            parentCollection:setRemoteUrl(parentGallery.full_url)
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
    local changed = gallery.parent_uuid ~= parentGalleryId or gallery.name ~= collectionInfo.name

    local collectionSettings
    local settingsChanged = false
    if updateSettings then
      -- User has edited the gallery settings (ie, description and/or display style), so update gallery if changed
      collectionSettings = collectionInfo.collectionSettings
      if collectionSettings then
        if collectionSettings["description"] ~= nil then
          settingsChanged = collectionSettings["description"] ~= (gallery.description or "")
        end
        if not settingsChanged and collectionSettings["display_style"] and collectionSettings["display_style"] ~= "" then
          settingsChanged = collectionSettings["display_style"] ~= gallery.gallery_display_style_uuid
        end

        changed = changed or settingsChanged
      end
    end

    if changed then
      gallery, error_msg = PhotoDeckAPI.updateGallery(urlname, gallery.uuid, parentGalleryId, collectionInfo, updateSettings)

      if not error_msg and settingsChanged then
        -- resynchronize LR published collection settings with the actual data in PhotoDeck
        collectionSettings["description"] = gallery.description
        collectionSettings["display_style"] = gallery.gallery_display_style_uuid
        collection.catalog:withWriteAccessDo("Resynchronize LR collection settings", function()
          if collection:type() == "LrPublishedCollection" then
            collection:setCollectionSettings(collectionSettings)
          elseif collection:type() == "LrPublishedCollectionSet" then
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
  gallery.full_url = galleryURL(website, gallery)
  if collection:getRemoteId() == nil or collection:getRemoteId() ~= gallery.uuid or collection:getRemoteUrl() ~= gallery.full_url then
    collection.catalog:withWriteAccessDo("Set Remote Id and Url", function()
      collection:setRemoteId(gallery.uuid)
      collection:setRemoteUrl(gallery.full_url)
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
  local website, websiteError = PhotoDeckAPI.website(urlname, true)
  local rootGalleryId = nil
  if website then
    rootGalleryId = website.root_gallery_uuid
  end

  if not rootGalleryId then
    PhotoDeckAPI.canSynchronize = true
    return nil, websiteError or LOC("$$$/PhotoDeck/API/Galleries/RootNotFound=Couldn't find PhotoDeck root gallery")
  end

  progressScope:setCaption(LOC("$$$/PhotoDeck/SynchronizeStatus/ReadingStructure=Reading PhotoDeck gallery structure"))
  local photodeckGalleries, error_msg = PhotoDeckAPI.galleries(urlname)

  if not photodeckGalleries or error_msg then
    PhotoDeckAPI.canSynchronize = true
    return nil, error_msg or LOC("$$$/PhotoDeck/SynchronizeStatus/ErrorReadingStructure=Couldn't get PhotoDeck gallery structure")
  end

  local photodeckGalleriesByParent = {}
  for uuid, gallery in pairs(photodeckGalleries) do
    if
      type(gallery.name) ~= "string"
      or type(gallery.full_url_path) ~= "string"
      or (gallery.medias_count ~= nil and type(gallery.medias_count) ~= "number")
    then
      PhotoDeckAPI.canSynchronize = true
      return nil, invalidResponse()
    end
    if gallery.parent_uuid == "" or not gallery.parent_uuid then
      gallery.parent_uuid = "NONE"
    end
    if not photodeckGalleriesByParent[gallery.parent_uuid] then
      photodeckGalleriesByParent[gallery.parent_uuid] = {}
    end
    photodeckGalleriesByParent[gallery.parent_uuid][uuid] = gallery
  end

  if not photodeckGalleries[rootGalleryId] then
    PhotoDeckAPI.canSynchronize = true
    return nil, LOC("$$$/PhotoDeck/API/Galleries/RootNotFound=Couldn't find PhotoDeck root gallery")
  end

  local synchronizeGallery
  synchronizeGallery = function(depth, parentPDGalleryUUID, parentLRCollectionSet)
    if progressScope:isCanceled() then
      return
    end

    local parentPDGallery = photodeckGalleries[parentPDGalleryUUID]
    progressScope:setCaption(LOC("$$$/PhotoDeck/SynchronizeStatus/Synchronizing=Synchronizing ^1", parentPDGallery.name))
    log_trace(string.format("SYNC: Exploring PhotoDeck galleries under %s '%s' at depth %i", parentPDGalleryUUID, parentPDGallery.name, depth))
    local pdGalleries = photodeckGalleriesByParent[parentPDGalleryUUID] or {}
    local lrCollectionSets = parentLRCollectionSet:getChildCollectionSets()
    local lrCollections = parentLRCollectionSet:getChildCollections()

    for uuid, gallery in pairs(pdGalleries) do
      gallery.full_url = galleryURL(website, gallery)
    end

    -- Scan Lightroom published collections, and connect them to PhotoDeck galleries
    local lrCollectionsByRemoteId = {}
    for _, lrCollection in pairs(lrCollections) do
      local rid = lrCollection:getRemoteId()
      if not rid or rid == "" then
        -- unconnected published collection, try to connect by name
        local lrCollectionName = lrCollection:getName()
        for uuid, gallery in pairs(pdGalleries) do
          if lrCollectionName == gallery.name then
            -- found matching gallery
            local collectionSettings = lrCollection:getCollectionInfoSummary().collectionSettings or {}
            catalog:withWriteAccessDo("Resynchronize LR collection settings", function()
              lrCollection:setRemoteId(uuid)
              lrCollection:setRemoteUrl(gallery.full_url)
              lrCollection:setCollectionSettings(collectionSettings)
            end)
            updateCount = updateCount + 1
            rid = uuid
            break
          end
        end
      end

      local gallery = pdGalleries[rid]
      if not gallery or gallery.parent_uuid ~= parentPDGalleryUUID then
        log_trace(
          string.format(
            "SYNC: Lightroom Published Collection %i '%s' is connected to PhotoDeck gallery %s, but it doesn't exist anymore. Deleting Published Collection.",
            lrCollection.localIdentifier,
            lrCollection:getName(),
            rid or "(none)"
          )
        )
        catalog:withWriteAccessDo("Deleting Published Collection", function()
          lrCollection:delete()
        end)
        deleteCount = deleteCount + 1
      elseif lrCollectionsByRemoteId[rid] then
        -- duplicate LR collections!
        local lrCollectionDup = lrCollectionsByRemoteId[rid]
        if gallery.name == lrCollectionDup:getName() then
          log_trace(
            string.format(
              "SYNC: Lightroom Published Collection %i '%s' is connected to PhotoDeck gallery %s '%s', but we already have Published Collection %i '%s' connected to it. Deleting the former.",
              lrCollection.localIdentifier,
              lrCollection:getName(),
              rid,
              gallery.name,
              lrCollectionDup.localIdentifier,
              lrCollectionDup:getName()
            )
          )
          catalog:withWriteAccessDo("Deleting Published Collection", function()
            lrCollection:delete()
          end)
          deleteCount = deleteCount + 1
        else
          log_trace(
            string.format(
              "SYNC: Lightroom Published Collection %i '%s' is connected to PhotoDeck gallery %s '%s', but we already have Published Collection %i '%s' connected to it. Deleting the later.",
              lrCollection.localIdentifier,
              lrCollection:getName(),
              rid,
              gallery.name,
              lrCollectionDup.localIdentifier,
              lrCollectionDup:getName()
            )
          )
          catalog:withWriteAccessDo("Deleting Published Collection", function()
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
      if not rid or rid == "" then
        -- unconnected published collection, try to connect by name
        local lrCollectionSetName = lrCollectionSet:getName()
        for uuid, gallery in pairs(pdGalleries) do
          if lrCollectionSetName == gallery.name then
            -- found matching gallery
            local collectionSettings = lrCollectionSet:getCollectionSetInfoSummary().collectionSettings or {}
            catalog:withWriteAccessDo("Resynchronize LR collection settings", function()
              lrCollectionSet:setRemoteId(uuid)
              lrCollectionSet:setRemoteUrl(gallery.full_url)
              lrCollectionSet:setCollectionSetSettings(collectionSettings)
            end)
            updateCount = updateCount + 1
            rid = uuid
            break
          end
        end
      end

      local gallery = pdGalleries[rid]
      if not gallery or gallery.parent_uuid ~= parentPDGalleryUUID then
        log_trace(
          string.format(
            "SYNC: Lightroom Published Collection Set %i '%s' is connected to PhotoDeck gallery %s, but it doesn't exist anymore. Deleting Published Collection Set.",
            lrCollectionSet.localIdentifier,
            lrCollectionSet:getName(),
            rid or "(none)"
          )
        )
        catalog:withWriteAccessDo("Deleting Published Collection Set", function()
          lrCollectionSet:delete()
        end)
        deleteCount = deleteCount + 1
      elseif lrCollectionSetsByRemoteId[rid] then
        -- duplicate LR collections sets!
        local lrCollectionSetd = lrCollectionSetsByRemoteId[rid]
        if gallery.name == lrCollectionSetd:getName() then
          log_trace(
            string.format(
              "SYNC: Lightroom Published Collection Set %i '%s' is connected to PhotoDeck gallery %s '%s', but we already have Published Collection Set %i '%s' connected to it. Deleting the former.",
              lrCollectionSet.localIdentifier,
              lrCollectionSet:getName(),
              rid,
              gallery.name,
              lrCollectionSetd.localIdentifier,
              lrCollectionSetd:getName()
            )
          )
          catalog:withWriteAccessDo("Deleting Published Collection Set", function()
            lrCollectionSet:delete()
          end)
          deleteCount = deleteCount + 1
        else
          log_trace(
            string.format(
              "SYNC: Lightroom Published Collection Set %i '%s' is connected to PhotoDeck gallery %s '%s', but we already have Published Collection Set %i '%s' connected to it. Deleting the later.",
              lrCollectionSet.localIdentifier,
              lrCollectionSet:getName(),
              rid,
              gallery.name,
              lrCollectionSetd.localIdentifier,
              lrCollectionSetd:getName()
            )
          )
          catalog:withWriteAccessDo("Deleting Published Collection Set", function()
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
      if progressScope:isCanceled() then
        return
      end

      local lrCollectionSet = lrCollectionSetsByRemoteId[uuid]
      local lrCollection = lrCollectionsByRemoteId[uuid]
      local shouldBeACollectionSet = photodeckGalleriesByParent[uuid]
      local shouldBeACollection = not shouldBeACollectionSet and (gallery.medias_count or 0) > 0

      if lrCollection and shouldBeACollectionSet then
        log_trace(
          string.format(
            "SYNC: PhotoDeck gallery %s '%s' is connected to Lightroom Published Collection %i, but it should be Publish Collection Set. Deleting Published Collection.",
            uuid,
            gallery.name,
            lrCollection.localIdentifier
          )
        )
        catalog:withWriteAccessDo("Deleting Published Collection", function()
          lrCollection:delete()
        end)
        deleteCount = deleteCount + 1
        lrCollection = nil
      end

      if lrCollectionSet and shouldBeACollection then
        log_trace(
          string.format(
            "SYNC: PhotoDeck gallery %s '%s' is connected to Lightroom Published Collection Set %i, but it should be Publish Collection. Deleting Published Collection Set.",
            uuid,
            gallery.name,
            lrCollectionSet.localIdentifier
          )
        )
        catalog:withWriteAccessDo("Deleting Published Collection Set", function()
          lrCollectionSet:delete()
        end)
        deleteCount = deleteCount + 1
        lrCollectionSet = nil
      end

      if lrCollection and lrCollectionSet then
        -- exists has both a Lightroom Published Collection and Published Collection Set. Choose the right type and delete the other.
        if shouldBeACollectionSet then
          log_trace(
            string.format(
              "SYNC: PhotoDeck gallery %s '%s' is connected to Lightroom Published Collection Set %i AND to Lightroom Published Collection %i, but it should be Publish Collection Set. Deleting Published Collection.",
              uuid,
              gallery.name,
              lrCollectionSet.localIdentifier,
              lrCollection.localIdentifier
            )
          )
          catalog:withWriteAccessDo("Deleting Published Collection", function()
            lrCollection:delete()
          end)
          deleteCount = deleteCount + 1
          lrCollection = nil
        elseif shouldBeACollection then
          log_trace(
            string.format(
              "SYNC: PhotoDeck gallery %s '%s' is connected to Lightroom Published Collection Set %i AND to Lightroom Published Collection %i, but it should be Publish Collection. Deleting Published Collection Set.",
              uuid,
              gallery.name,
              lrCollectionSet.localIdentifier,
              lrCollection.localIdentifier
            )
          )
          catalog:withWriteAccessDo("Deleting Published Collection Set", function()
            lrCollectionSet:delete()
          end)
          deleteCount = deleteCount + 1
          lrCollectionSet = nil
        else
          log_trace(
            string.format(
              "SYNC: PhotoDeck gallery %s '%s' is connected to Lightroom Published Collection Set %i AND to Lightroom Published Collection %i, and we don't know yet what it should be. Assuming Published Collection, and deleting Published Collection Set.",
              uuid,
              gallery.name,
              lrCollectionSet.localIdentifier,
              lrCollection.localIdentifier
            )
          )
          catalog:withWriteAccessDo("Deleting Published Collection", function()
            lrCollection:delete()
          end)
          deleteCount = deleteCount + 1
          lrCollectionSet = nil
        end
      end

      if lrCollectionSet then
        -- Already properly connected, good
        local collectionSettings = lrCollectionSet:getCollectionSetInfoSummary().collectionSettings or {}
        if lrCollectionSet:getRemoteUrl() ~= gallery.full_url then
          log_trace(
            string.format(
              "SYNC: PhotoDeck gallery %s '%s' already connected to Lightroom Published Collection Set %i. Updating.",
              uuid,
              gallery.name,
              lrCollectionSet.localIdentifier
            )
          )
          catalog:withWriteAccessDo("Resynchronize LR collection settings", function()
            lrCollectionSet:setRemoteUrl(gallery.full_url)
            lrCollectionSet:setCollectionSetSettings(collectionSettings)
          end)
          updateCount = updateCount + 1
        else
          log_trace(
            string.format(
              "SYNC: PhotoDeck gallery %s '%s' already connected to Lightroom Published Collection Set %i. Doing nothing.",
              uuid,
              gallery.name,
              lrCollectionSet.localIdentifier
            )
          )
        end
      elseif lrCollection then
        -- Already properly connected, good
        local collectionSettings = lrCollection:getCollectionInfoSummary().collectionSettings or {}
        if lrCollection:getRemoteUrl() ~= gallery.full_url then
          log_trace(
            string.format(
              "SYNC: PhotoDeck gallery %s '%s' already connected to Lightroom Published Collection %i. Updating.",
              uuid,
              gallery.name,
              lrCollection.localIdentifier
            )
          )
          catalog:withWriteAccessDo("Resynchronize LR collection settings", function()
            lrCollection:setRemoteUrl(gallery.full_url)
            lrCollection:setCollectionSettings(collectionSettings)
          end)
          updateCount = updateCount + 1
        else
          log_trace(
            string.format(
              "SYNC: PhotoDeck gallery %s '%s' already connected to Lightroom Published Collection %i. Doing nothing.",
              uuid,
              gallery.name,
              lrCollection.localIdentifier
            )
          )
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
          collectionName = collectionName .. " (" .. tostring(copyCount) .. ")"
        end

        if shouldBeACollectionSet then
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' NOT found in Lightroom. Creating Published Collection Set.", uuid, collectionName))
          catalog:withWriteAccessDo("Creating Published Collection Set", function()
            lrCollectionSet = publishService:createPublishedCollectionSet(collectionName, parentLRCollectionSet)
          end)
          if lrCollectionSet then
            local collectionSettings = {}
            catalog:withWriteAccessDo("Set LR collection settings", function()
              lrCollectionSet:setRemoteId(uuid)
              lrCollectionSet:setRemoteUrl(gallery.full_url)
              lrCollectionSet:setCollectionSetSettings(collectionSettings)
            end)
            createCount = createCount + 1
          else
            log_trace(
              string.format(
                "SYNC ERROR: PhotoDeck gallery %s '%s' NOT found in Lightroom, and failed to create Published Collection Set.",
                uuid,
                collectionName
              )
            )
            errorsCount = errorsCount + 1
          end
        elseif shouldBeACollection then
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' NOT found in Lightroom. Creating Published Collection.", uuid, collectionName))
          catalog:withWriteAccessDo("Creating Published Collection", function()
            lrCollection = publishService:createPublishedCollection(collectionName, parentLRCollectionSet)
          end)
          if lrCollection then
            local collectionSettings = {}
            catalog:withWriteAccessDo("Set LR collection settings", function()
              lrCollection:setRemoteId(uuid)
              lrCollection:setRemoteUrl(gallery.full_url)
              lrCollection:setCollectionSettings(collectionSettings)
            end)
            createCount = createCount + 1
          else
            log_trace(
              string.format("SYNC ERROR: PhotoDeck gallery %s '%s' NOT found in Lightroom, and failed to create Published Collection.", uuid, collectionName)
            )
            errorsCount = errorsCount + 1
          end
        else
          log_trace(string.format("SYNC: PhotoDeck gallery %s '%s' NOT found in Lightroom. Creating Published Collection by default.", uuid, collectionName))
          catalog:withWriteAccessDo("Creating Published Collection", function()
            lrCollection = publishService:createPublishedCollection(collectionName, parentLRCollectionSet)
          end)
          if lrCollection then
            local collectionSettings = {}
            catalog:withWriteAccessDo("Set LR collection settings", function()
              lrCollection:setRemoteId(uuid)
              lrCollection:setRemoteUrl(gallery.full_url)
              lrCollection:setCollectionSettings(collectionSettings)
            end)
            createCount = createCount + 1
          else
            log_trace(
              string.format("SYNC ERROR: PhotoDeck gallery %s '%s' NOT found in Lightroom, and failed to create Published Collection.", uuid, collectionName)
            )
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
  local url = "/medias/" .. PhotoDeckHttp.encode(photoId) .. ".json"
  local onerror = {}
  onerror["404"] = function()
    return nil
  end
  return requestObject("GET", url, { include = "gallery_uuids" }, onerror)
end

function PhotoDeckAPI.photosInGallery(urlname, galleryId)
  log_trace(string.format('PhotoDeckAPI.photosInGallery("%s", "%s")', urlname, galleryId))
  return requestList(galleryPath(urlname, galleryId), { include = "media_uuids", per_page = 1000 }, nil, nil, "media_uuids")
end

function PhotoDeckAPI.subGalleriesInGallery(urlname, galleryId, matchingName, ignore_not_found)
  log_trace(string.format('PhotoDeckAPI.subGalleriesInGallery("%s", "%s")', urlname, galleryId))
  local url = galleryPath(urlname, galleryId):gsub("%.json$", "/subgalleries.json")
  local onerror = {}
  if ignore_not_found then
    onerror["404"] = function()
      return nil
    end
  end

  local params = { per_page = 500 }
  if matchingName then
    params["filter[name]"] = matchingName
  end
  local galleries, error_msg = requestList(url, params, nil, onerror)
  if not galleries then
    return nil, error_msg
  end
  local subgalleries = {}
  for uuid, gallery in pairs(galleries) do
    if gallery.parent_uuid == galleryId then
      subgalleries[uuid] = gallery
    end
  end
  return subgalleries
end

local function buildMediaMetadataParams(photo, updating)
  local params = {}
  local title = photo:getFormattedMetadata("headline")
  if not title or title == "" then
    title = photo:getFormattedMetadata("title")
  end
  params.title = title
  params.description = photo:getFormattedMetadata("caption")
  params.keywords = photo:getFormattedMetadata("keywordTagsForExport")
  params.location = photo:getFormattedMetadata("location")
  params.city = photo:getFormattedMetadata("city")
  params.state = photo:getFormattedMetadata("stateProvince")
  params.country = photo:getFormattedMetadata("country")
  params.author = photo:getFormattedMetadata("creator")
  params.copyright = photo:getFormattedMetadata("copyright")
  local location = photo:getFormattedMetadata("locationShown") or photo:getFormattedMetadata("locationCreated")
  if location and location[1] then
    params.region = location[1]["WorldRegion"]
  end
  local artist_rating = tonumber(photo:getFormattedMetadata("rating"))
  if updating and not artist_rating then
    params.delete_artist_rating = true
  else
    params.artist_rating = artist_rating
  end
  params.date_created = photo:getRawMetadata("dateTimeOriginalISO8601")
  return params
end

local function buildMediaUploadParams(contentPath, mime_type, file_size)
  local failedUploadLocations = currentContext().failedUploadLocations
  local params = {
    upload_location = "REQUEST",
    file_name = LrPathUtils.leafName(contentPath),
    file_size = file_size,
    mime_type = mime_type,
    capabilities = "raw",
  }
  if #failedUploadLocations > 0 then
    params.failed_locations = table.concat(failedUploadLocations, ",")
  end
  return params
end

function PhotoDeckAPI.uploadPhoto(urlname, attributes)
  return PhotoDeckAPI.updatePhoto(nil, urlname, attributes)
end

function PhotoDeckAPI.updatePhoto(photoId, urlname, attributes, handleNotFound)
  local failedUploadLocations = currentContext().failedUploadLocations
  local onerror = {}
  if handleNotFound then
    onerror["404"] = function()
      return nil, "Not found"
    end
  end
  local upload_attempts = 0

  while true do
    local url = photoId and "/medias/" .. PhotoDeckHttp.encode(photoId) .. ".json" or "/medias.json"
    local method = photoId and "PUT" or "POST"
    local mediaParams = {}
    local params = { media = mediaParams }
    local upload_location_requested = false
    local file_size
    local mime_type
    if attributes.contentPath then
      local fileAttributes = LrFileUtils.fileAttributes(attributes.contentPath)
      if not fileAttributes or not fileAttributes.fileSize then
        return nil, LOC("$$$/PhotoDeck/API/FileReadError=Error reading file"), false
      end
      mime_type = attributes.mimeType or "application/octet-stream"
      file_size = fileAttributes.fileSize
      mediaParams.content = buildMediaUploadParams(attributes.contentPath, mime_type, file_size)
      upload_location_requested = true
      upload_attempts = upload_attempts + 1
      if attributes.watermarkId then
        mediaParams.watermark_uuid = attributes.watermarkId
      end
    end
    if attributes.contentUploadLocation then
      mediaParams.content = {
        upload_location = attributes.contentUploadLocation,
        file_name = attributes.contentFileName,
        file_size = attributes.contentFileSize,
        mime_type = attributes.contentMimeType,
        upload_duration = attributes.uploadDuration,
      }
    end
    if attributes.artistId then
      params.artist_uuid = attributes.artistId
    end
    if attributes.publishToGallery then
      mediaParams.publish_to_gallery_uuids = { attributes.publishToGallery }
    end
    if attributes.injectMetadata and attributes.lrPhoto then
      local metadataParams = buildMediaMetadataParams(attributes.lrPhoto, not not photoId)
      for k, v in pairs(metadataParams) do
        mediaParams[k] = v
      end
    end
    local media, error_msg, info = requestObject(method, url, params, onerror)
    if handleNotFound and error_msg == "Not found" then
      return { notfound = true }, error_msg, false
    end

    if error_msg then
      return nil, error_msg, not not (info and info.body and info.body.stop_with_error == true)
    end
    if not media or type(media.uuid) ~= "string" or media.uuid == "" then
      return nil, invalidResponse(), false
    end
    photoId = media.uuid
    if not upload_location_requested then
      http.delayNext(0.25)
      return media, nil, false
    end
    if type(media.upload_url) ~= "string" or media.upload_url == "" then
      return nil, LOC("$$$/PhotoDeck/API/Media/UploadURLMissing=Upload URL missing"), false
    end
    if
      type(media.upload_method) ~= "string"
      or type(media.upload_location) ~= "string"
      or type(media.file_name) ~= "string"
      or type(media.mime_type) ~= "string"
      or (media.upload_file_param ~= nil and (type(media.upload_file_param) ~= "string" or media.upload_file_param == ""))
      or (media.upload_params ~= nil and not JSON.isObject(media.upload_params))
      or (media.upload_expires_in ~= nil and type(media.upload_expires_in) ~= "number")
    then
      return nil, invalidResponse(), false
    end
    local expiresAt = media.upload_expires_in and LrDate.currentTime() + media.upload_expires_in
    local startedAt = LrDate.currentTime()
    local expired = false
    for attempt = 1, 3 do
      if expiresAt and LrDate.currentTime() >= expiresAt then
        expired = true
        error_msg = LOC("$$$/PhotoDeck/API/Media/UploadExpired=Upload authorization expired")
        break
      end
      local success, retryable
      success, error_msg, retryable = http.transfer(media, attributes.contentPath, file_size)
      if success then
        -- Confirmation failure must not cause another transfer of the file.
        return PhotoDeckAPI.updatePhoto(photoId, urlname, {
          contentUploadLocation = media.upload_location,
          contentFileName = media.file_name,
          contentFileSize = file_size,
          contentMimeType = media.mime_type,
          uploadDuration = LrDate.currentTime() - startedAt,
        }, false)
      end
      if not retryable then
        if expiresAt and LrDate.currentTime() >= expiresAt then
          expired = true
          break
        end
        return nil, error_msg, false
      end
      if attempt < 3 then
        LrTasks.sleep(2 ^ attempt)
      end
    end
    if upload_attempts >= 3 then
      return nil, error_msg, false
    end
    local alreadyFailed = false
    for _, location in ipairs(failedUploadLocations) do
      alreadyFailed = alreadyFailed or location == media.upload_location
    end
    if not expired and not alreadyFailed then
      failedUploadLocations[#failedUploadLocations + 1] = media.upload_location
    end
  end
end

function PhotoDeckAPI.deletePhoto(photoId)
  log_trace(string.format('PhotoDeckAPI.deletePhoto("%s")', photoId))
  local onerror = {}
  onerror["404"] = function()
    return nil
  end
  return PhotoDeckAPI.request("DELETE", "/medias/" .. PhotoDeckHttp.encode(photoId) .. ".json", nil, onerror)
end

local function batchUpdate(photoIds, task, value, acceptedCallback)
  local ids, seen = {}, {}
  -- Validate the entire selection before submitting any destructive work.
  for _, id in ipairs(photoIds) do
    if type(id) ~= "string" or id == "" or id == "all" or id:find("[,%s]") then
      return nil, LOC("$$$/PhotoDeck/API/InvalidSelection=Invalid media selection")
    end
    if not seen[id] then
      ids[#ids + 1], seen[id] = id, true
    end
  end
  local response = true
  for start = 1, #ids, 3000 do
    local chunk = {}
    for index = start, math.min(start + 2999, #ids) do
      chunk[#chunk + 1] = ids[index]
    end
    local params = {
      select = table.concat(chunk, ","),
      medias = { [task] = value },
    }
    local error_msg, info
    response, error_msg, info = requestObject("PATCH", "/medias.json", params)
    if error_msg then
      return nil, error_msg
    end
    if not info or info.status ~= 202 then
      return nil, invalidResponse()
    end
    -- Acknowledge each chunk once it is accepted for background processing.
    if acceptedCallback then
      for _, id in ipairs(chunk) do
        acceptedCallback(id)
      end
    end
  end
  return response
end

function PhotoDeckAPI.deletePhotos(photoIds, acceptedCallback)
  return batchUpdate(photoIds, "delete", true, acceptedCallback)
end

function PhotoDeckAPI.unpublishPhoto(photoId, galleryId)
  log_trace(string.format('PhotoDeckAPI.unpublishPhoto("%s", "%s")', photoId, galleryId))
  local url = "/medias/" .. PhotoDeckHttp.encode(photoId) .. ".json"
  local params = { media = { unpublish_from_gallery_uuids = { galleryId } } }
  local onerror = {}
  onerror["404"] = function()
    return nil
  end
  return requestObject("PUT", url, params, onerror)
end

function PhotoDeckAPI.unpublishPhotos(photoIds, galleryId, acceptedCallback)
  return batchUpdate(photoIds, "unpublish_from_gallery_uuids", { galleryId }, acceptedCallback)
end

function PhotoDeckAPI.galleryDisplayStyles(urlname)
  log_trace(string.format('PhotoDeckAPI.galleryDisplayStyles("%s")', urlname))
  return cached("gallery_display_styles/" .. urlname, function()
    local result, error_msg = requestList("/websites/" .. PhotoDeckHttp.encode(urlname) .. "/gallery_display_styles.json", { per_page = 1000 })
    if result and not next(result) then
      return nil, LOC("$$$/PhotoDeck/API/GalleryDisplayStyles/Empty=Couldn't get list of gallery display styles")
    end
    return result, error_msg
  end)
end

function PhotoDeckAPI.deleteGallery(urlname, galleryId)
  log_trace(string.format('PhotoDeckAPI.deleteGallery("%s", "%s")', urlname, galleryId))
  return PhotoDeckAPI.request("DELETE", galleryPath(urlname, galleryId))
end

function PhotoDeckAPI.reorderGallery(urlname, galleryId, mediasIds)
  local params = { gallery = { content_order = "manual-last", media_uuids_order = table.concat(mediasIds, ",") } }
  return requestObject("PUT", galleryPath(urlname, galleryId), params)
end

-- Done
return PhotoDeckAPI
