return {
  LrSdkVersion = 4.1,
  LrSdkMinimumVersion = 4.1, -- URL callbacks and streaming HTTP uploads
  LrToolkitIdentifier = "com.photodeck.dev.lightroom-publish",
  LrPluginName = "PhotoDeck dev",
  LrPluginInfoUrl = "https://github.com/cmaion/photodeck.lrdevplugin",
  URLHandler = "PhotoDeckURLHandler.lua",

  LrExportServiceProvider = {
    title = "PhotoDeck", -- this string appears in the Publish Services panel
    file = "PhotoDeckPublishServiceProvider.lua", -- the service definition script
  },
  LrMetadataProvider = "PhotoDeckMetadataDefinition.lua",
  VERSION = { major = 0, minor = 20, revision = 1 },
}
