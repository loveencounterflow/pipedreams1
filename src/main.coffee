

############################################################################################################
njs_util                  = require 'util'
#...........................................................................................................
TEXT                      = require 'coffeenode-text'
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'PIPEDREAMS/main'
log                       = TRM.get_logger 'plain',     badge
info                      = TRM.get_logger 'info',      badge
whisper                   = TRM.get_logger 'whisper',   badge
alert                     = TRM.get_logger 'alert',     badge
debug                     = TRM.get_logger 'debug',     badge
warn                      = TRM.get_logger 'warn',      badge
help                      = TRM.get_logger 'help',      badge
urge                      = TRM.get_logger 'urge',      badge
echo                      = TRM.echo.bind TRM
rainbow                   = TRM.rainbow.bind TRM
#...........................................................................................................
### https://github.com/dominictarr/event-stream ###
ES                        = require 'event-stream'
#...........................................................................................................
### http://c2fo.github.io/fast-csv/index.html, https://github.com/C2FO/fast-csv ###
S                         = require 'string'
#...........................................................................................................
@create_readstream        = require './create-readstream'


############################################################################################################
# GENERIC METHODS
#-----------------------------------------------------------------------------------------------------------
@as_transformer = @$ = ( method ) -> ES.map method




############################################################################################################
# DELETION
#-----------------------------------------------------------------------------------------------------------
@$skip_empty = ->
  return $ ( record, handler ) =>
    return handler() if record.length is 0
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$limit = ( limit = 1 ) ->
  count = 0
  return @$ ( record, handler ) =>
    count += 1
    return handler null, null if count > limit
    handler null, record #, { foo: 42, id: record[ 'id' ] + 'XXX' }


############################################################################################################
# DISPLAY / REPORTING
#-----------------------------------------------------------------------------------------------------------
@$show_and_quit = ->
  return @$ ( record, handler ) =>
    info rpr record
    warn 'aborting from `TRANSFORMERS.show_and_quit`'
    setImmediate -> process.exit()
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$show = ->
  return @$ ( record, handler ) =>
    info rpr record
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$show_sample = ( input_stream ) ->
  ### TAINT may introduce a memory leak. ###
  records = []
  input_stream.once 'end', =>
    whisper '©5r0', "displaying random record out of #{records.length}"
    debug   '©5r0', rpr records[ Math.floor Math.random() * records.length ]
  return @$ ( record, handler ) =>
    records.push record
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$count = ( input_stream, title ) ->
  count = 0
  input_stream.on 'end', ->
    urge ( title ? 'Count' ) + ':', count
  return @$ ( record, handler ) =>
    count += 1
    handler null, record


#===========================================================================================================
# CSV
#-----------------------------------------------------------------------------------------------------------
@$parse_csv = ->
  field_names = null
  return $ ( record, handler ) =>
    values = ( S record ).parseCSV ',', '"', '\\'
    if field_names is null
      field_names = values
      return handler()
    record = {}
    record[ field_names[ idx ] ] = value for value, idx in values
    handler null, record


#===========================================================================================================
# PODs
#-----------------------------------------------------------------------------------------------------------
@$as_pods = ->
  record_idx  = -1
  field_names = null
  #.........................................................................................................
  return @$ ( record, handler ) =>
    # whisper record.join ''
    if ( record_idx += 1 ) is 0
      field_names = record
      return
    R = {}
    for field_value, field_idx in record
      field_name      = field_names[ field_idx ]
      R[ field_name ] = field_value
    handler null, R

#-----------------------------------------------------------------------------------------------------------
@$delete_prefix = ( prefix ) ->
  if      njs_util.isRegExp prefix then starts_with = ( text ) -> prefix.test text
  else if njs_util.isString prefix then starts_with = ( text ) -> ( me.lastIndexOf probe, 0 ) is 0
  else throw new Error "need a RegEx or a text as prefix"
  #.........................................................................................................
  return @$ ( record, handler ) =>
    for old_field_name, field_value of record
      continue unless starts_with old_field_name, prefix
      new_field_name =  old_field_name.replace prefix, ''
      continue if new_field_name.length is 0
      ### TAINT should throw error ###
      continue if record[ new_field_name ]?
      record[ new_field_name ] = field_value
      delete record[ old_field_name ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$dasherize_field_names = ->
  return @$ ( record, handler ) =>
    for old_field_name of record
      new_field_name = old_field_name.replace /_/g, '-'
      continue if new_field_name is old_field_name
      @_rename record, old_field_name, new_field_name
    #.......................................................................................................
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@_rename = ( record, old_field_name, new_field_name ) ->
  @_copy record, old_field_name, new_field_name, 'rename'
  delete record[ old_field_name ]
  return record

#-----------------------------------------------------------------------------------------------------------
@_copy = ( record, old_field_name, new_field_name, action ) ->
  #.........................................................................................................
  if record[ old_field_name ] is undefined
    error = new Error """
      when trying to #{action} field #{rpr old_field_name} to #{rpr new_field_name}
      found that there is no field #{rpr old_field_name} in
      #{rpr record}"""
    error[ 'code' ] = 'no such field'
    throw error
  #.........................................................................................................
  if record[ new_field_name ] isnt undefined
    throw new Error """
      when trying to #{action} field #{rpr old_field_name} to #{rpr new_field_name}
      found that field #{rpr new_field_name} already present in
      #{rpr record}"""
    error[ 'code' ] = 'duplicate field'
    throw error
  #.........................................................................................................
  record[ new_field_name ] = record[ old_field_name ]
  return record

#-----------------------------------------------------------------------------------------------------------
@$rename = ( old_field_name, new_field_name ) ->
  return @$ ( record, handler ) =>
    handler null, @_rename record, old_field_name, new_field_name

#-----------------------------------------------------------------------------------------------------------
@$copy = ( old_field_name, new_field_name ) ->
  return @$ ( record, handler ) =>
    handler null, @_copy record, old_field_name, new_field_name, 'copy'

#-----------------------------------------------------------------------------------------------------------
@$set = ( field_name, field_value ) ->
  return @$ ( record, handler ) =>
    record[ field_name ] = field_value
    handler null, record

