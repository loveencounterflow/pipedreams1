

############################################################################################################
njs_fs                    = require 'fs'
#...........................................................................................................
# TRM                       = require 'coffeenode-trm'
# rpr                       = TRM.rpr.bind TRM
# badge                     = 'PIPEDREAMS/create-readstream'
# log                       = TRM.get_logger 'plain',     badge
# info                      = TRM.get_logger 'info',      badge
# whisper                   = TRM.get_logger 'whisper',   badge
# alert                     = TRM.get_logger 'alert',     badge
# debug                     = TRM.get_logger 'debug',     badge
# warn                      = TRM.get_logger 'warn',      badge
# help                      = TRM.get_logger 'help',      badge
# urge                      = TRM.get_logger 'urge',      badge
# echo                      = TRM.echo.bind TRM
#...........................................................................................................
### https://github.com/visionmedia/node-progress ###
ProgressBar               = require 'progress'
#...........................................................................................................
after                     = ( time_s, f ) -> setTimeout f, time_s * 1000


#-----------------------------------------------------------------------------------------------------------
module.exports = create_readstream = ( route, label ) ->
  ### Create and return a new instance of a read stream form a single route or a list of routes. In the
  latter case, a combined stream using https://github.com/felixge/node-combined-stream is constructed
  so that several files (that are presumable the result of an earlier split operation that was done to
  reduce individual file sizes) transparently like one huge single file.

  As a bonus, the module uses https://github.com/visionmedia/node-progress to display a progress bar
  for reading operations that last for more than a couple seconds.

  # As a second bonus, the module uses CoffeeNode's `TRM.listen_to_keys` method to implement a `ctrl-C,
  # ctrl-C`-style abort shortcut with an informative message displayed when `ctrl-C` has been hit by the user
  # once; this is to prevent longish read operations to be inadvertantly terminated.
  ###
  #.........................................................................................................
  switch type = TYPES.type_of route
    when 'text'
      R = njs_fs.createReadStream route
    when 'list'
      routes          = route
      ### https://github.com/felixge/node-combined-stream ###
      CombinedStream  = require 'combined-stream'
      R               = CombinedStream.create()
      for partial_route in routes
        R.append njs_fs.createReadStream partial_route
    else
      throw new Error "unable to create readstream for argument of type #{rpr type}"
  #.........................................................................................................
  size            = get_filesize route
  collected_bytes = 0
  bar_is_shown    = no
  is_first_call   = yes
  format          = "[:bar] :percent | :current / #{size} | +:elapseds -:etas #{label}"
  #.........................................................................................................
  options   =
    width:      50
    total:      size
    complete:   '#'
    incomplete: '—'
  #.........................................................................................................
  R.on 'data', ( data ) ->
    collected_bytes += data.length
    if bar_is_shown
      bar.tick if is_first_call then collected_bytes else data.length
      is_first_call = no
  #.........................................................................................................
  bar   = new ProgressBar format, options
  timer = after 3, -> bar_is_shown = yes
  #.........................................................................................................
  R.on 'end', -> clearTimeout timer
  # TRM.listen_to_keys key_listener
  #.........................................................................................................
  return R

# #-----------------------------------------------------------------------------------------------------------
# key_listener = ( key ) ->
#   echo() if key is '\u0003'

#-----------------------------------------------------------------------------------------------------------
get_filesize = ( route ) ->
  return ( njs_fs.statSync route ).size unless Array.isArray route
  R = 0
  R += get_filesize partial_route for partial_route in route
  return R



