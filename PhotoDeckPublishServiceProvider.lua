local LrApplication = import 'LrApplication'
local LrFileUtils = import 'LrFileUtils'
local LrTasks = import 'LrTasks'
local LrErrors = import 'LrErrors'
local LrHttp = import 'LrHttp'

local logger = import 'LrLogger'( 'PhotoDeckPublishLightroomPlugin' )
logger:enable('logfile')

local PhotoDeckAPI = require 'PhotoDeckAPI'
local PhotoDeckDialogs = require 'PhotoDeckDialogs'

local publishServiceProvider = {}

-- General plugin configuration for export & publish operations
publishServiceProvider.hideSections = { 'exportLocation' }
publishServiceProvider.allowFileFormats = { 'JPEG', 'TIFF', 'Original' }
publishServiceProvider.hidePrintResolution = true


-- General plugin configuration for publish operations
publishServiceProvider.supportsIncrementalPublish = true
publishServiceProvider.supportsCustomSortOrder = true
publishServiceProvider.small_icon = 'photodeck16.png'
publishServiceProvider.titleForPublishedCollection = LOC("$$$/PhotoDeck/Publish/Collection=Gallery")
publishServiceProvider.titleForPublishedCollectionSet = LOC("$$$/PhotoDeck/Publish/CollectionSet=Folder")
publishServiceProvider.titleForGoToPublishedCollection = LOC("$$$/PhotoDeck/Publish/GoToCollection=Go to PhotoDeck Gallery")
publishServiceProvider.titleForGoToPublishedPhoto =  LOC("$$$/PhotoDeck/Publish/GoToPhoto=Go to Photo in PhotoDeck Gallery")


-- Photo metadata changes that should retrigger a republish
publishServiceProvider.metadataThatTriggersRepublish = function(publishSettings)
  return {
    default = false,
    rating = true,
    title = true,
    caption = true,
    creator = true,
    keywords = true,
    dateCreated = true,
    headline = true,
    iptcSubjectCode = true,
    dateCreated = true,
    location = true,
    city = true,
    stateProvince = true,
    country = true,
    copyright = true,
    customMetadata = false
  }
end


-- These fields get stored between uses
publishServiceProvider.exportPresetFields = {
  { key = 'username', default = "" },
  { key = 'password', default = "" },
  { key = 'apiKey', default = "" },
  { key = 'apiSecret', default = "" },
  { key = 'websiteChosen', default = "" },
  { key = 'uploadOnRepublish', default = false }
}


-- Plugin settings dialog
publishServiceProvider.startDialog = PhotoDeckDialogs.startDialog
publishServiceProvider.sectionsForTopOfDialog = PhotoDeckDialogs.sectionsForTopOfDialog


-- Dialog when a publish service has been created
publishServiceProvider.didCreateNewPublishService = PhotoDeckDialogs.didCreateNewPublishService


-- Published collection / collection set settings dialog
publishServiceProvider.viewForCollectionSettings = PhotoDeckDialogs.viewForCollectionSettings
publishServiceProvider.viewForCollectionSetSettings = PhotoDeckDialogs.viewForCollectionSettings


-- Published collection name validation
publishServiceProvider.validatePublishedCollectionName = function(proposedName)
  -- string needs to be valid UTF-8 (3 multibyte chars max) and less than 200 bytes
  local length = string.len(proposedName)
  return length > 0 and length < 200
end


-- Process rendered photos (export or publish)
local function getPhotoDeckPhotoIdsStoredInCatalog(photo)
  local str = photo:getPropertyForPlugin(_PLUGIN, "photoId")
  if not str then
    return {}
  end

  local res = {}
  for elem in string.gmatch(str, "%S+") do
    local key = nil
    local val = nil
    local i = 0
    local pkv = {}
    for kv in string.gmatch(elem, '[^:]+') do
      i = i + 1
      pkv[i] = kv
    end
    if pkv[1] then
      if pkv[2] then
        key = pkv[1]
        val = pkv[2]
      else
        key = ''
        val = pkv[1]
      end
    end
    if key then
      res[key] = val
    end
  end

  return res
end

local function storePhotoDeckPhotoIdsInCatalog(catalog, photo, websiteuuid, photouuid)
  local curr = getPhotoDeckPhotoIdsStoredInCatalog(photo)
  curr[websiteuuid] = photouuid
  local str = ''
  for k, v in pairs(curr) do
    if k and k ~= '' then
      str = str .. tostring(k) .. ':' .. tostring(v) .. ' '
    else
      str = str .. tostring(v) .. ' '
    end
  end

  local prev = photo:getPropertyForPlugin(_PLUGIN, "photoId")
  if prev ~= str then
    catalog:withWriteAccessDo( "publish", function( context )
      photo:setPropertyForPlugin(_PLUGIN, "photoId", str)
    end)
  end
end


function publishServiceProvider.processRenderedPhotos( functionContext, exportContext )
  logger:trace('publishServiceProvider.processRenderedPhotos')

  local exportSession = exportContext.exportSession
  local exportSettings = assert( exportContext.propertyTable )
  local isPublish = exportSettings.LR_isExportForPublish
  local nPhotos = exportSession:countRenditions()
  local catalog = exportSession.catalog
  local error_msg
  local cancel_next_uploads = false

  PhotoDeckAPI.connect(exportSettings.apiKey, exportSettings.apiSecret, exportSettings.username, exportSettings.password)

  -- Set progress title.
  local progressScope = exportContext:configureProgress {
    title = nPhotos > 1
    and LOC("$$$/PhotoDeck/ProcessRenderedPhotos/Progress=Publishing ^1 photos to PhotoDeck", nPhotos)
    or LOC "$$$/PhotoDeck/ProcessRenderedPhotos/Progress/One=Publishing one photo to PhotoDeck",
  }

  -- Look for a gallery id for this collection.
  local urlname = exportSettings.websiteChosen
  local gallery = nil
  local website
  local websiteuuid = nil

  if isPublish then
    website, error_msg = PhotoDeckAPI.website(urlname)
    if not website or error_msg then
      progressScope:done()
      LrErrors.throwUserError(LOC("$$$/PhotoDeck/ProcessRenderedPhotos/ErrorGettingWebsite=Error retrieving website: ^1", error_msg))
      return
    end
    websiteuuid = website.uuid
    local collectionInfo = exportContext.publishedCollectionInfo
    local galleryId = collectionInfo.remoteId
    local galleryPhotos

    if galleryId then
      gallery, error_msg = PhotoDeckAPI.gallery(urlname, galleryId, true)
      if error_msg then
        progressScope:done()
        LrErrors.throwUserError(LOC("$$$/PhotoDeck/ProcessRenderedPhotos/ErrorGettingGallery=Error retrieving gallery: ^1", error_msg))
        return
      end
    end

    if not gallery then
      -- Create or update this gallery.
      if not collectionInfo.publishedCollection then
        collectionInfo.publishedCollection = exportContext.publishedCollection
      end
      gallery, error_msg = PhotoDeckAPI.createOrUpdateGallery(urlname, collectionInfo)
      if error_msg then
        progressScope:done()
        LrErrors.throwUserError(LOC("$$$/PhotoDeck/ProcessRenderedPhotos/ErrorCreatingGallery=Error creating gallery: ^1", error_msg))
        return
      end
    end
  end

  -- Iterate through photo renditions.
  for i, rendition in exportContext:renditions { stopIfCanceled = true } do
    -- Update progress scope.
    progressScope:setPortionComplete( ( i - 1 ) / nPhotos )

    -- Get next photo.
    local photo = rendition.photo
    local photoId = nil
    local catalogKey = nil

    if not rendition.wasSkipped and not cancel_next_uploads then
      -- See if we previously uploaded this photo.
      if isPublish then
        local isVirtualCopy = photo:getRawMetadata('isVirtualCopy')
        if isVirtualCopy then
          -- virtual copy shares the same metadata catalog entries it seems, so we use a different key to store the PhotoDeck ID
          catalogKey = websiteuuid .. "/" .. tostring(photo.localIdentifier)
        else
          catalogKey = websiteuuid
        end

        -- get photo ID from previous upload (either in this gallery or another one)
        -- NOTE: we are using rendition.publishedPhotoId only as a last resort, as the photo might have seen it's UUID change in another gallery (e.g., following a manual deletion directly within PhotoDeck, followed by a publication in another gallery) -- rendition.publishedPhotoId might thus be outdated
        catalog:withReadAccessDo( function()
          local photoIds = getPhotoDeckPhotoIdsStoredInCatalog(photo)
          if isVirtualCopy then
            photoId = photoIds[catalogKey] or rendition.publishedPhotoId
          else
            photoId = photoIds[catalogKey] or photoIds[''] or rendition.publishedPhotoId
          end
        end)
      end

      local success, pathOrMessage = rendition:waitForRender()
      -- Update progress scope again once we've got rendered photo.
      progressScope:setPortionComplete( ( i - 0.5 ) / nPhotos )
      -- Check for cancellation again after photo has been rendered.
      if progressScope:isCanceled() then break end
      if success then
        error_msg = nil

        local photoAlreadyPublished = not not photoId

        local upload
        if not error_msg then
          -- Build list of photo attributes
          local photoAttributes = {}

          if not photoAlreadyPublished or exportSettings.uploadOnRepublish then
            photoAttributes.contentPath = pathOrMessage
          end
          if gallery then
            photoAttributes.publishToGallery = gallery.uuid
          end
          photoAttributes.lrPhoto = photo

          -- Upload or replace/update the photo.
          if photoAlreadyPublished then
            upload, error_msg, cancel_next_uploads = PhotoDeckAPI.updatePhoto(photoId, urlname, photoAttributes, true)
            if upload and upload.notfound then
              -- Not found error on PhotoDeck. Assume that the photo is gone and that we need to upload it again.
              photoAlreadyPublished = false
              photoAttributes.contentPath = pathOrMessage
            end
          end

          if not photoAlreadyPublished then
            upload, error_msg, cancel_next_uploads = PhotoDeckAPI.uploadPhoto(urlname, photoAttributes)
          end
        end

        if not error_msg and upload and upload.uuid and upload.uuid ~= "" then
          if upload.filename and upload.filename ~= "" and upload.filename ~= photo:getPropertyForPlugin(_PLUGIN, "fileName") then
            -- Store PhotoDeck file name
            catalog:withPrivateWriteAccessDo(function(context)
              photo:setPropertyForPlugin(_PLUGIN, "fileName", upload.filename)
            end)
          end

          if isPublish then
            -- Also save the remote photo ID at the LrPhoto level, so that we can find it when publishing in a different gallery
            storePhotoDeckPhotoIdsInCatalog(catalog, photo, catalogKey, upload.uuid)

            -- Mark all instances of this Lightroom Photo within our published collections as being clean (not edited).
            -- E.g., when metadata have been changed, re-publishing from any of the published collection will update the PhotoDeck photo for all. No need to re-publish from each published collection.
            if photoAlreadyPublished then
              for _, publishedCollection in pairs(photo:getContainedPublishedCollections()) do
                if publishedCollection:getService().localIdentifier == exportContext.publishService.localIdentifier then
                  for _, publishedPhotoCopy in pairs(publishedCollection:getPublishedPhotos()) do
                    if publishedPhotoCopy:getPhoto().localIdentifier == photo.localIdentifier and publishedPhotoCopy:getEditedFlag() then
                      catalog:withWriteAccessDo("Marking photo as clean", function( context )
                        publishedPhotoCopy:setEditedFlag(false)
                      end)
                      break
                    end
                  end
                end
              end
            end

            rendition:recordPublishedPhotoId(upload.uuid)
          end
        else
          rendition:uploadFailed(error_msg or LOC("$$$/PhotoDeck/ProcessRenderedPhotos/ErrorUploading=Upload failed"))
        end

        -- When done with photo, delete temp file. There is a cleanup step that happens later,
        -- but this will help manage space in the event of a large upload.
        LrFileUtils.delete( pathOrMessage )
      end

    elseif cancel_next_uploads then
      rendition:renditionIsDone(false, error_msg or LOC("$$$/PhotoDeck/ProcessRenderedPhotos/ErrorUploading=Upload failed"))
    end
  end

  progressScope:done()
end



-- Delete photo from published collection
publishServiceProvider.deletePhotosFromPublishedCollection = function(publishSettings, arrayOfPhotoIds, deletedCallback, localCollectionId)
  logger:trace('publishServiceProvider.deletePhotosFromPublishedCollection')
  PhotoDeckAPI.connect(publishSettings.apiKey, publishSettings.apiSecret, publishSettings.username, publishSettings.password)
  local catalog = LrApplication.activeCatalog()
  local collection = catalog:getPublishedCollectionByLocalIdentifier(localCollectionId)
  local galleryId = collection:getRemoteId()
  local photoIdsToDelete = {}
  local photoIdsToUnpublish = {}
  local result
  local error_msg
  -- this next bit is stupid. Why is there no catalog:getPhotoByRemoteId or similar
  local publishedPhotos = collection:getPublishedPhotos()
  local publishedPhotoById = {}
  for _, pp in pairs(publishedPhotos) do
    publishedPhotoById[pp:getRemoteId()] = pp
  end
  for i, photoId in ipairs( arrayOfPhotoIds ) do
    error_msg = nil
    if photoId ~= "" then
      local publishedPhoto = publishedPhotoById[photoId]

      local collCount = 0
      for _, c in pairs(publishedPhoto:getPhoto():getContainedPublishedCollections()) do
        if c:getRemoteId() ~= galleryId then
          collCount = collCount + 1
        end
      end

      if collCount == 0 then
        -- delete photo if this is the only collection it's in
        table.insert(photoIdsToDelete, photoId)
      else
        -- otherwise unpublish from the passed in collection
        table.insert(photoIdsToUnpublish, photoId)
      end
    end
  end

  -- Unpublish
  local photoIdsToUnpublishCount = #photoIdsToUnpublish
  if photoIdsToUnpublishCount == 1 then
    -- Only one photo needs to be unpublished. Use non batched API endpoint.
    local photoId = photoIdsToUnpublish[1]
    result, error_msg = PhotoDeckAPI.unpublishPhoto(photoId, galleryId)

    if error_msg then
      LrErrors.throwUserError(LOC("$$$/PhotoDeck/DeletePhotos/ErrorUnpublishingPhoto=Error unpublishing photo: ^1", error_msg))
    else
      deletedCallback(photoId)
    end
  elseif photoIdsToUnpublishCount > 0 then
    -- More than one photo needs to be unpublished. Use batched API endpoint.
    result, error_msg = PhotoDeckAPI.unpublishPhotos(photoIdsToUnpublish, galleryId)

    if error_msg then
      LrErrors.throwUserError(LOC("$$$/PhotoDeck/DeletePhotos/ErrorUnpublishingPhotos=Error unpublishing photos: ^1", error_msg))
    else
      for _, photoId in ipairs(photoIdsToUnpublish) do
        deletedCallback(photoId)
      end
    end
  end

  -- Delete
  local photoIdsToDeleteCount = #photoIdsToDelete
  if photoIdsToDeleteCount == 1 then
    -- Only one photo needs to be deleted. Use non batched API endpoint.
    local photoId = photoIdsToDelete[1]
    result, error_msg = PhotoDeckAPI.deletePhoto(photoId)

    if error_msg then
      LrErrors.throwUserError(LOC("$$$/PhotoDeck/DeletePhotos/ErrorDeletingPhoto=Error deleting photo: ^1", error_msg))
    else
      deletedCallback(photoId)
    end
  elseif photoIdsToDeleteCount > 0 then
    -- More than one photo needs to be deleted. Use batched API endpoint.
    result, error_msg = PhotoDeckAPI.deletePhotos(photoIdsToDelete)

    if error_msg then
      LrErrors.throwUserError(LOC("$$$/PhotoDeck/DeletePhotos/ErrorDeletingPhotos=Error deleting photos: ^1", error_msg))
    else
      for _, photoId in ipairs(photoIdsToDelete) do
        deletedCallback(photoId)
      end
    end
  end
end


-- Update published collection
publishServiceProvider.updateCollectionSettings = function( publishSettings, info )
  logger:trace('publishServiceProvider.updateCollectionSettings')
  PhotoDeckAPI.connect(publishSettings.apiKey, publishSettings.apiSecret, publishSettings.username, publishSettings.password)
  local urlname = publishSettings.websiteChosen
  local result, error_msg = PhotoDeckAPI.createOrUpdateGallery(urlname, info, true)
  if error_msg then
    LrErrors.throwUserError(LOC("$$$/PhotoDeck/UpdateCollection/ErrorUpdatingGallery=Error updating gallery: ^1", error_msg))
  end
end


-- Update published collection set
publishServiceProvider.updateCollectionSetSettings = function( publishSettings, info )
  logger:trace('publishServiceProvider.updateCollectionSetSettings')
  PhotoDeckAPI.connect(publishSettings.apiKey, publishSettings.apiSecret, publishSettings.username, publishSettings.password)
  local urlname = publishSettings.websiteChosen
  local result, error_msg = PhotoDeckAPI.createOrUpdateGallery(urlname, info, true)
  if error_msg then
    LrErrors.throwUserError(LOC("$$$/PhotoDeck/UpdateCollection/ErrorUpdatingGallery=Error updating gallery: ^1", error_msg))
  end
end


-- Rename published collection
publishServiceProvider.renamePublishedCollection = function( publishSettings, info )
  logger:trace('publishServiceProvider.renamePublishedCollection')
  PhotoDeckAPI.connect(publishSettings.apiKey, publishSettings.apiSecret, publishSettings.username, publishSettings.password)
  local urlname = publishSettings.websiteChosen
  local result, error_msg = PhotoDeckAPI.createOrUpdateGallery(urlname, info)
  if error_msg then
    LrErrors.throwUserError(LOC("$$$/PhotoDeck/UpdateCollection/ErrorRenamingGallery=Error renaming gallery: ^1", error_msg))
  end
end


-- Reparent published collection
publishServiceProvider.reparentPublishedCollection = function( publishSettings, info )
  logger:trace('publishServiceProvider.reparentPublishedCollection')
  PhotoDeckAPI.connect(publishSettings.apiKey, publishSettings.apiSecret, publishSettings.username, publishSettings.password)
  local urlname = publishSettings.websiteChosen
  local result, error_msg = PhotoDeckAPI.createOrUpdateGallery(urlname, info)
  if error_msg then
    LrErrors.throwUserError(LOC("$$$/PhotoDeck/UpdateCollection/ErrorReparentingGallery=Error reparenting gallery: ^1", error_msg))
  end
end


-- Delete published collection
publishServiceProvider.deletePublishedCollection = function( publishSettings, info )
  logger:trace('publishServiceProvider.deletePublishedCollection')
  PhotoDeckAPI.connect(publishSettings.apiKey, publishSettings.apiSecret, publishSettings.username, publishSettings.password)
  local urlname = publishSettings.websiteChosen
  local galleryId = info.remoteId
  local result, error_msg = PhotoDeckAPI.deleteGallery(urlname, galleryId)
  if error_msg then
    LrErrors.throwUserError(LOC("$$$/PhotoDeck/DeleteCollection/ErrorDeletingGallery=Error deleting gallery: ^1", error_msg))
  end
end


-- Go to published photo
publishServiceProvider.goToPublishedPhoto = function( publishSettings, info )
  logger:trace('publishServiceProvider.goToPublishedPhoto')
  local catalog = LrApplication.activeCatalog()

  -- The following is just ugly and not robust, but Lightroom doesn't gives us the LrPublishedCollection object, just it's name and parents.
  -- So we need to go to it's parent and find the parent children that matches the collection name...
  local publishedCollectionParent = info.publishService
  for _, parent in pairs(info.publishedCollectionInfo.parents) do
    publishedCollectionParent = catalog:getPublishedCollectionByLocalIdentifier(parent.localCollectionId)
  end
  local publishedCollection = nil
  for _, collection in pairs(publishedCollectionParent:getChildCollections()) do
    if collection:getName() == info.publishedCollectionInfo.name then
      publishedCollection = collection
      break
    end
  end

  if publishedCollection then
    local galleryurl = publishedCollection:getRemoteUrl()
    if galleryurl and galleryurl ~= '' then
      local url = galleryurl .. '/-/medias/' .. info.remoteId
      logger:trace('Opening ' .. url)
      LrHttp.openUrlInBrowser(url)
    else
      LrErrors.throwUserError(LOC("$$$/PhotoDeck/GoToPublishedPhoto/GalleryUrlNotFound=Gallery address is not known yet"))
    end
  else
    LrErrors.throwUserError(LOC("$$$/PhotoDeck/GoToPublishedPhoto/CollectionNotFound=Error finding collection"))
  end
end


-- Reorder collection
publishServiceProvider.imposeSortOrderOnPublishedCollection = function( publishSettings, info, remoteIdSequence )
  logger:trace('publishServiceProvider.imposeSortOrderOnPublishedCollection')
  PhotoDeckAPI.connect(publishSettings.apiKey, publishSettings.apiSecret, publishSettings.username, publishSettings.password)

  local urlname = publishSettings.websiteChosen
  local galleryId = info.remoteCollectionId
  local response
  local error_msg

  response, error_msg = PhotoDeckAPI.reorderGallery(urlname, galleryId, remoteIdSequence)
  if error_msg then
    LrErrors.throwUserError(LOC("$$$/PhotoDeck/UpdateCollection/ErrorReorderingGallery=Error reordering gallery: ^1", error_msg))
    return false
  else
    return true
  end
end


-- Done
return publishServiceProvider
