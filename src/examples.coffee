

############################################################################################################
njs_util                  = require 'util'
#...........................................................................................................
TYPES                     = require 'coffeenode-types'
TEXT                      = require 'coffeenode-text'
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'PIPEDREAMS/examples'
log                       = TRM.get_logger 'plain',     badge
info                      = TRM.get_logger 'info',      badge
whisper                   = TRM.get_logger 'whisper',   badge
alert                     = TRM.get_logger 'alert',     badge
debug                     = TRM.get_logger 'debug',     badge
warn                      = TRM.get_logger 'warn',      badge
help                      = TRM.get_logger 'help',      badge
urge                      = TRM.get_logger 'urge',      badge
echo                      = TRM.echo.bind TRM
#...........................................................................................................
P                         = require './main'
ASYNC                     = require 'async'

#-----------------------------------------------------------------------------------------------------------
@show_throughstream = ->
  #.........................................................................................................
  confluence = P.create_throughstream()
    #.......................................................................................................
    .pipe P.remit ( record, send ) =>
      if record?
        [ task_id, n, ] = record
        debug task_id, n
        send [ task_id, n * 2, ]
    #.......................................................................................................
    .pipe P.$show()
  #.........................................................................................................
  tasks = []
  stop  = 14
  for start in [ 3, 9, 12, ]
    task_id = tasks.length
    do ( task_id, start ) =>
      tasks.push ( handler ) =>
        for x in [ start .. stop ]
          confluence.write [ task_id, x, ]
        handler null
  #.........................................................................................................
  ASYNC.parallel tasks, ( error ) =>
    throw error if error?
    help 'ok'
  #.........................................................................................................
  return null


#-----------------------------------------------------------------------------------------------------------
@main = ->
  @show_throughstream()

############################################################################################################
@main() unless module.parent?

