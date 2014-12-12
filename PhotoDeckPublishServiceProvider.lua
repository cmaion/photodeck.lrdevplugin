-- local LrMobdebug = import 'LrMobdebug'
local LrBinding = import 'LrBinding'
local LrDialogs = import 'LrDialogs'
local LrView = import 'LrView'
local LrTasks = import 'LrTasks'

local logger = import 'LrLogger'( 'PhotoDeckPublishServiceProvider' )

logger:enable('print')

local PhotoDeckAPI = require 'PhotoDeckAPI'
local PhotoDeckUtils = require 'PhotoDeckUtils'
local printTable = PhotoDeckUtils.printTable

local exportServiceProvider = {}

-- needed to publish in addition to export
exportServiceProvider.supportsIncrementalPublish = true
-- exportLocation gets replaced with PhotoDeck specific form section
exportServiceProvider.hideSections = { 'exportLocation' }
exportServiceProvider.small_icon = 'photodeck16.png'

-- these fields get stored between uses
exportServiceProvider.exportPresetFields = {
  { key = 'username', default = "" },
  { key = 'password', default = "" },
  { key = 'fullname', default = "" },
  { key = 'apiKey', default = "" },
  { key = 'apiSecret', default = "" },
  { key = 'website', default = "" },
}

local function  updateApiKeyAndSecret(propertyTable)
  local f = LrView.osFactory()
  local c = f:column {
    bind_to_object = propertyTable,
    spacing = f:dialog_spacing(),
    f:row {
      f:static_text {
        title = "API Key",
        width = LrView.share "label_width",
        alignment = "right",
      },
      f:edit_field {
        value = LrView.bind 'apiKey',
        immediate = false,
      }
    },
    f:row {
      f:static_text {
        title = "API Secret",
        width = LrView.share "label_width",
        alignment = "right",
      },
      f:edit_field {
        value = LrView.bind 'apiSecret',
        immediate = false,
      },
    },
  }
  local result = LrDialogs.presentModalDialog({
    title = LOC "$$$/PhotoDeck/APIKeys=PhotoDeck API Keys",
    contents = c,
  })
  return propertyTable
end

local function ping(propertyTable)
  propertyTable.pingResult = 'making api call'
  PhotoDeckAPI.connect(propertyTable.apiKey, propertyTable.apiSecret)
  LrTasks.startAsyncTask(function()
    propertyTable.pingResult = PhotoDeckAPI.ping()
  end, 'PhotoDeckAPI Ping')
end

local function login(propertyTable)
  propertyTable.loggedinResult = 'logging in...'
  PhotoDeckAPI.connect(propertyTable.apiKey,
       propertyTable.apiSecret, propertyTable.username, propertyTable.password)
  LrTasks.startAsyncTask(function()
    local result = PhotoDeckAPI.whoami()
    propertyTable.loggedin = true
    propertyTable.loggedinResult = 'Logged in as ' .. result.firstname .. ' ' .. result.lastname
  end, 'PhotoDeckAPI Login')
end

local function getWebsites(propertyTable)
  PhotoDeckAPI.connect(propertyTable.apiKey,
       propertyTable.apiSecret, propertyTable.username, propertyTable.password)
  LrTasks.startAsyncTask(function()
    propertyTable.websiteChoices = PhotoDeckAPI.websites()
  end, 'PhotoDeckAPI Get Websites')
end

local function showGalleries(propertyTable)
  PhotoDeckAPI.connect(propertyTable.apiKey,
       propertyTable.apiSecret, propertyTable.username, propertyTable.password)
  LrTasks.startAsyncTask(function()
    PhotoDeckAPI.galleries(propertyTable.websiteChosen)
  end, 'PhotoDeckAPI Get Websites')
end

local function showGalleries(propertyTable)
  PhotoDeckAPI.connect(propertyTable.apiKey,
       propertyTable.apiSecret, propertyTable.username, propertyTable.password)
  logger:trace(propertyTable.websiteChosen)
  LrTasks.startAsyncTask(function()
    PhotoDeckAPI.galleries(propertyTable.websiteChosen)
  end, 'PhotoDeckAPI Get Websites')
end

function exportServiceProvider.startDialog(propertyTable)
  propertyTable.loggedin = false
  if propertyTable.apiKey == '' or propertyTable.apiSecret == '' then
    propertyTable = updateApiKeyAndSecret(propertyTable)
  end
  ping(propertyTable)
  if #propertyTable.username and #propertyTable.password and
    #propertyTable.apiKey and #propertyTable.apiSecret then
    login(propertyTable)
    getWebsites(propertyTable)
  end
end

function exportServiceProvider.sectionsForTopOfDialog( f, propertyTable )
  -- LrMobdebug.on()
  propertyTable.pingResult = 'Awaiting instructions'
  propertyTable.loggedinResult = 'Not logged in'

  local apiCredentials =  {
    title = LOC "$$$/PhotoDeck/ExportDialog/Account=PhotoDeck Plugin API keys",
    synopsis = LrView.bind 'pingResult',

    f:row {
      bind_to_object = propertyTable,
      f:column {
        f:row {
          f:static_text {
            title = "API Key:",
            width = LrView.share "label_width",
            alignment = 'right'
          },
          f:static_text {
            title = LrView.bind 'apiKey',
            width_in_chars = 40,
          }
        },
        f:row {
          f:static_text {
            title = "API Secret:",
            width = LrView.share "label_width",
            alignment = 'right'
          },
          f:static_text {
            title = LrView.bind 'apiSecret',
            width_in_chars = 40,
          }
        },
      },
      f:column {
        f:push_button {
          title = 'Update',
          enabled = true,
          action = function()
            propertyTable = updateApiKeyAndSecret(propertyTable)
          end,
        },
        f:static_text {
          title = LrView.bind 'pingResult',
          alignment = 'right',
          fill_horizontal = 1,
        },
      },
    },
  }
  local userCredentials = {
    title = LOC "$$$/PhotoDeck/ExportDialog/Account=PhotoDeck Account",
    synopsis = LrView.bind 'loggedinResult',

    f:row {
      bind_to_object = propertyTable,
      f:column {
        f:row {
          f:static_text {
            title = "Username:",
            width = LrView.share "user_label_width",
            alignment = 'right'
          },
          f:edit_field {
            value = LrView.bind 'username',
            width_in_chars = 20,
          }
        },
        f:row {
          f:static_text {
            title = "Password:",
            width = LrView.share "user_label_width",
            alignment = 'right'
          },
          f:password_field {
            value = LrView.bind 'password',
            width_in_chars = 20,
          }
        },
      },

      f:push_button {
        width = tonumber( LOC "$$$/locale_metric/PhotoDeck/ExportDialog/TestButton/Width=90" ),
        title = 'Login',
        enabled = LrBinding.negativeOfKey('loggedin'),
        action = function () login(propertyTable) end
      },

      f:static_text {
        title = LrView.bind 'loggedinResult',
        alignment = 'right',
        fill_horizontal = 1,
        height_in_lines = 1,
      },

    },
  }
  local websiteChoice = {
    title = LOC "$$$/PhotoDeck/ExportDialog/Account=PhotoDeck Website",
    synopsis = LrView.bind 'websiteChosen',

    f:row {
      bind_to_object = propertyTable,

      f:push_button {
        enabled = true,
        title = 'Get websites',
        action = function () getWebsites(propertyTable) end
      },

      f:popup_menu {
        title = "Select Website",
        items = LrView.bind 'websiteChoices',
        value = LrView.bind 'websiteChosen',
      },
      f:push_button {
        enabled = true,
        title = 'Show galleries',
        action = function () showGalleries(propertyTable) end
      }

    }
  }

  return {
    apiCredentials,
    userCredentials,
    websiteChoice,
  }
end

return exportServiceProvider
