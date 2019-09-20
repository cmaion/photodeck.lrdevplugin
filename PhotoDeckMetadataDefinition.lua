return {
  metadataFieldsForPhotos = {
    {
      id = "photoId",
      datatype = "string",
    },
    {
      id = "fileName",
      datatype = "string",
      title = LOC "$$$/PhotoDeck/MetaData/PhotoDeckFileName=File Name",
      readOnly = true,
      searchable = true,
      browsable = true
    }
  },
  schemaVersion = 2,
  updateFromEarlierSchemaVersion = function( catalog, previousSchemaVersion, progressScope )
  end
}
