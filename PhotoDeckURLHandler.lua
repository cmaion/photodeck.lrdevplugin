return {
  URLHandler = function(url)
    require("PhotoDeckOAuth").receiveCallback(url)
  end,
}
