

############################################################################################################
njs_util                  = require 'util'
#...........................................................................................................
TYPES                     = require 'coffeenode-types'
TEXT                      = require 'coffeenode-text'
CHR                       = require 'coffeenode-chr'
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
### http://stringjs.com ###
S                         = require 'string'
#...........................................................................................................
HELPERS                   = require './HELPERS'
#...........................................................................................................
### https://github.com/loveencounterflow/copypaste ###
COPYPASTE                 = require 'copypaste'
#...........................................................................................................
### https://github.com/dominictarr/level-live-stream ###
create_levellivestream    = require 'level-live-stream'
#...........................................................................................................
### https://github.com/dominictarr/sort-stream ###
@$sort                    = require 'sort-stream'
# #...........................................................................................................
# ### https://github.com/dominictarr/pull-stream ###
# pull_stream               = require 'pull-stream'
# #...........................................................................................................
# ### https://github.com/dominictarr/pull-stream-to-stream ###
# pull_stream_to_stream     = require 'pull-stream-to-stream'


#===========================================================================================================
# GENERIC METHODS
#-----------------------------------------------------------------------------------------------------------
@create_readstream            = HELPERS.create_readstream             .bind HELPERS
@create_readstream_from_text  = HELPERS.create_readstream_from_text   .bind HELPERS
@pimp_readstream              = HELPERS.pimp_readstream               .bind HELPERS
# @$                            = ES.map                                .bind ES
# @map                          = ES.mapSync                            .bind ES
@merge                        = ES.merge                              .bind ES
@$split                       = ES.split                              .bind ES
@$chain                       = ES.pipeline                           .bind ES
@through                      = ES.through                            .bind ES
@duplex                       = ES.duplex                             .bind ES
@as_readable                  = ES.readable                           .bind ES
@read_list                    = ES.readArray                          .bind ES
@eos                          = { 'eos': true }

@$ = ->
  alert 'P.$ deprecated'
  throw new Error 'deprecated'
@map = ->
  alert 'P.map deprecated'
  throw new Error 'deprecated'

#===========================================================================================================
#
#-----------------------------------------------------------------------------------------------------------
@resume = ( stream ) ->
  setImmediate -> stream.resume()
  return stream

#-----------------------------------------------------------------------------------------------------------
@$comment = ( message, valediction ) ->
  message_shown = no
  return @remit ( data, send, end ) ->
    unless message_shown
      help message if message?
      message_shown = yes
    send data
    if end?
      help valediction if valediction?
      end()

#-----------------------------------------------------------------------------------------------------------
@$split_chrs = ( settings ) ->
  decoder     = null
  settings   ?= {}
  #.........................................................................................................
  return @remit ( data, send ) =>
    #.......................................................................................................
    if TYPES.isa_text data
      text      = data
    #.......................................................................................................
    else
      decoder  ?= new ( require 'string_decoder' ).StringDecoder settings[ 'encoding' ] ? 'utf8'
      text      = decoder.write data
    #.......................................................................................................
    for chr in CHR.chrs_from_text text, settings
      send chr

#===========================================================================================================
# SPECIALIZED STREAMS
#-----------------------------------------------------------------------------------------------------------
@create_throughstream = ( P... ) ->
  R = ES.through P...
  R.setMaxListeners 0
  return R

#-----------------------------------------------------------------------------------------------------------
@create_clipstream = ( interval_ms = 500 ) ->
  last_content = null
  #.........................................................................................................
  R = @create_throughstream()
    # .pipe @remit ( content, send ) =>
    #   debug '©r334', content
    #   send content
  #.........................................................................................................
  capture = =>
    COPYPASTE.paste ( _, content ) =>
      if content? and content isnt last_content and content.length > 0
        R.write content
        last_content = content
  #.........................................................................................................
  setInterval capture, interval_ms
  return R

#-----------------------------------------------------------------------------------------------------------
@echo_clipstream = ( interval_ms ) ->
  return @create_clipstream interval_ms
    .pipe @remit ( text, send ) =>
      echo text

#-----------------------------------------------------------------------------------------------------------
@create_levelstream = ( level_db ) ->
  R = create_levellivestream level_db
  # R.setMaxListeners 0
  return R

# #-----------------------------------------------------------------------------------------------------------
# @create_pullstream = ( P... ) ->
#   ### TAINT experimental ###
#   return pull_stream_to_stream pull_stream.values P...

#===========================================================================================================
# ERROR HANDLING
#-----------------------------------------------------------------------------------------------------------
###

Also see https://github.com/juliangruber/multipipe

# thx to http://grokbase.com/t/gg/nodejs/12bwd4zm4x/should-stream-pipe-forward-errors#20121130d5o2xbrhk3llhaxbasu5n374ke

Stream = ( require 'stream' ).Stream
Stream::pipeErr = (dest, opt) ->
  fw = dest.emit.bind(dest, "error")
  @on "error", fw
  self = this
  dest.on "unpipe", (src) ->
    dest.removeListener "error", fw  if src is self
    return
  return @pipe dest, opt
###

#===========================================================================================================
# TRANSFORMERS
#-----------------------------------------------------------------------------------------------------------
@remit = ( method ) ->
  send      = null
  cache     = null
  on_end    = null
  #.........................................................................................................
  get_send = ( self ) ->
    R         = (  data ) -> self.emit 'data',  data # if data?
    R.error   = ( error ) -> self.emit 'error', error
    R.end     =           -> self.emit 'end'
    return R
  #.........................................................................................................
  switch arity = method.length
    #.......................................................................................................
    when 2
      #.....................................................................................................
      on_data = ( data ) ->
        # debug '©3w9', send
        send = get_send @ unless send?
        method data, send
    #.......................................................................................................
    when 3
      cache = []
      #.....................................................................................................
      on_data = ( data ) ->
        # debug '©3w9', send, data
        if cache.length is 0
          cache[ 0 ] = data
          return
        send = get_send @ unless send?
        [ cache[ 0 ], data, ] = [ data, cache[ 0 ], ]
        method data, send, null
      #.....................................................................................................
      on_end = ->
        send  = get_send @ unless send?
        end   = => @emit 'end'
        if cache.length is 0
          data = null
        else
          data = cache[ 0 ]
          cache.length = 0
        method data, send, end
    #.......................................................................................................
    else
      throw new Error "expected a method with an arity of 2 or 3, got one with an arity of #{arity}"
  #.........................................................................................................
  return ES.through on_data, on_end


#===========================================================================================================
# DELETION
#-----------------------------------------------------------------------------------------------------------
@$skip_empty = ->
  return @remit ( record, send ) =>
    send record if record.length > 0

#-----------------------------------------------------------------------------------------------------------
### TAINT rename to `$take` ###
### TAINT end stream when limit reached ###
@$skip_after = ( limit = 1 ) ->
  count = 0
  return @remit ( record, send ) =>
    count += 1
    send record if count <= limit

#-----------------------------------------------------------------------------------------------------------
@$take = ( limit = 1 ) ->
  count = 0
  return @remit ( record, send ) =>
    count += 1
    if count <= limit then  send record
    else                    send.end()


#-----------------------------------------------------------------------------------------------------------
# @$stop = -> @remit ( data, send ) -> send.end()

#-----------------------------------------------------------------------------------------------------------
@$trim = ->
  return @remit ( line, send ) =>
    send line.trim() if line?

#-----------------------------------------------------------------------------------------------------------
@$skip_comments = ( marker = '#' ) ->
  ### TAINT does only work after trimming ###
  return @remit ( line, send ) =>
    send line if line? and line[ 0 ] isnt marker

#-----------------------------------------------------------------------------------------------------------
@$skip_comments_2 = ->
  ### TAINT makeshift method awaiting a better solution ###
  matcher = /^\s*#/
  return @remit ( line, send ) =>
    send line if line? and not matcher.test line

#-----------------------------------------------------------------------------------------------------------
@$sink = ->
  sink  = ( require 'fs' ).createWriteStream '/dev/null'
  R = @create_throughstream()
    .pipe @remit ( data, send ) ->
      # process.stdout.write '0'
      send '0'
    .pipe sink
  return R


#===========================================================================================================
# SAMPLING / THINNING OUT
#-----------------------------------------------------------------------------------------------------------
@$sample = ( p = 0.5, options ) ->
  ### Given a `0 <= p <= 1`, interpret `p` as the *p*robability to *p*ick a given record and otherwise toss
  it, so that `$sample 1` will keep all records, `$sample 0` will toss all records, and
  `$sample 0.5` (the default) will toss (on average) every other record.

  You can pipe several `$sample()` calls, reducing the data stream to 50% with each step. If you know
  your data set has, say, 1000 records, you can cut down to a random sample of 10 by piping the result of
  calling `$sample 1 / 1000 * 10` (or, of course, `$sample 0.01`).

  Tests have shown that a data file with 3'722'578 records (which didn't even fit into memory when parsed)
  could be perused in a matter of seconds with `$sample 1 / 1e4`, delivering a sample of around 370
  records. Because these records are randomly selected and because the process is so immensely sped up, it
  becomes possible to develop regular data processing as well as coping strategies for data-overload
  symptoms with much more ease as compared to a situation where small but realistic data sets are not
  available or have to be produced in an ad-hoc, non-random manner.

  **Parsing CSV**: There is a slight complication when your data is in a CSV-like format: in that case,
  there is, with `0 < p < 1`, a certain chance that the *first* line of a file is tossed, but some
  subsequent lines are kept. If you start to transform the text line into objects with named values later in
  the pipe (which makes sense, because you will typically want to thin out largeish streams as early on as
  feasible), the first line kept will be mis-interpreted as a header line (which must come first in CSV
  files) and cause all subsequent records to become weirdly malformed. To safeguard against this, use
  `$sample p, headers: true` (JS: `$sample( p, { headers: true } )`) in your code.

  **Predictable Samples**: Sometimes it is important to have randomly selected data where samples are
  constant across multiple runs:

  * once you have seen that a certain record appears on the screen log, you are certain it will be in the
    database, so you can write a snippet to check for this specific one;

  * you have implemented a new feature you want to test with an arbitrary subset of your data. You're
    still tweaking some parameters and want to see how those affect output and performance. A random
    sample that is different on each run would be a problem because the number of records and the sheer
    bytecount of the data may differ from run to run, so you wouldn't be sure which effects are due to
    which causes.

  To obtain predictable samples, use `$sample p, seed: 1234` (with a non-zero number of your choice);
  you will then get the exact same
  sample whenever you re-run your piping application with the same stream and the same seed. An interesting
  property of the predictable sample is that—everything else being the same—a sample with a smaller `p`
  will always be a subset of a sample with a bigger `p` and vice versa. ###
  #.........................................................................................................
  unless 0 <= p <= 1
    throw new Error "expected a number between 0 and 1, got #{rpr p}"
  #.........................................................................................................
  ### Handle trivial edge cases faster (hopefully): ###
  return ( @remit ( record, send ) => send record ) if p == 1
  return ( @remit ( record, send ) => null        ) if p == 0
  #.........................................................................................................
  headers = options?[ 'headers'     ] ? false
  seed    = options?[ 'seed'        ] ? null
  count   = 0
  rnd     = rnd_from_seed seed
  #.........................................................................................................
  return @remit ( record, send ) =>
    count += 1
    send record if ( count is 1 and headers ) or rnd() < p

#-----------------------------------------------------------------------------------------------------------
@$throttle_bytes = ( bytes_per_second ) ->
  Throttle = require 'throttle'
  return new Throttle bytes_per_second

#-----------------------------------------------------------------------------------------------------------
@$throttle_items = ( items_per_second ) ->
  new_gate  = require 'floodgate'
  return new_gate interval: 1 / items_per_second

#-----------------------------------------------------------------------------------------------------------
@$pass = ->
  return @remit ( data, send ) -> send data if data?

#-----------------------------------------------------------------------------------------------------------
@$unique = ( as_key ) ->
  ### Given an optional `as_key` method, return a transformer that skips all repeated items in the stream.
  If `as_key` is given, it is called repeated with the stream data and expected to return an identifying
  text (or other datatypes which can be meaningfully used as an JS object key, such as an integer); which
  items to skip will be based on that key. If `as_key` is not given, then the streamed data itself is
  used as key (which, in case all the data instances strigify as `[Object object]` or similar, means that
  only the very first piece of data will make it through—probably not what you want). ###
  seen = {}
  return @remit ( data, send ) =>
    key = if as_key? then as_key data else data
    unless seen[ key ]?
      seen[ key ] = 1
      send data

#-----------------------------------------------------------------------------------------------------------
@$filter = ( filter ) ->
  ### Given a `filter` method, return a stream transformer that will call `r = filter data` with each data
  instance that comes down the stream and only re-send that data if `r` is strictly `true`. If `r` is
  neither strictly `true` nor strictly `false`, an error will be sent to avoid silent failure. ###
  return @remit ( data, send ) =>
    switch r = filter data
      when true   then send data
      when false  then null
      else send.error new Error "illegal filter result #{rpr r}"

#===========================================================================================================
# COLLECTING
#-----------------------------------------------------------------------------------------------------------
@$batch = ( n, handler ) ->
  ### TAINT check for meaningful n ###
  ### TAINT make signal configurable ###
  collector     = []
  received_eos  = false
  eos           = @eos
  #.........................................................................................................
  if handler?
    #.........................................................................................................
    on_data = ( record ) ->
      if record is eos
        received_eos  = true
      else
        collector.push record
        if collector.length >= n
          handler null, collector
          collector = []
      @emit 'data', record
    #.........................................................................................................
    on_end  = ->
      handler null, collector if collector.length > 0
      handler null, eos       if received_eos
      @emit 'end'
  #.........................................................................................................
  else
    #.........................................................................................................
    on_data = ( record ) ->
      if record is eos
        received_eos  = true
      else
        collector.push record
        if collector.length >= n
          @emit 'data', collector
          collector = []
    #.........................................................................................................
    on_end  = ->
      @emit 'data', collector if collector.length > 0
      @emit 'data', eos       if received_eos
      @emit 'end'
  #.........................................................................................................
  return ES.through on_data, on_end

#-----------------------------------------------------------------------------------------------------------
@$collect_sample = ( n, options, result_handler ) ->
  ### Given an `input_stream`, a positive integer number `n`, (facultatively) options, and a `handler`, try
  to assemble a representative sample with up to `n` records from the stream. When the stream has ended,
  the handler is called once with (a `null` error argument and) a list of records.

  Similarly to `PIPEDREAMS.$sample`, it is possible to pass in a `headers: true` option to make sure the
  headers line of a CSV file is not collected. Also similarly, a `seed: 1234` argument can be used to
  ensure that the sample is arbitrary but constant for the same stream and the same seed.

  Observe that while `$sample` does thin out the stream, `$collect_sample` will never add anything to or
  omit anything from the stream; in that respect, it is rather more similar to `$collect`.

  The (simple) algorithm this method uses to arrive at a representative, fixed-size sample from a collection
  of unknown size has been kindly provided by two guys on
  [Math StackExchange](http://math.stackexchange.com/q/890272/168522). ###
  switch arity = arguments.length
    when 2
      result_handler  = options
      headers         = false
      seed            = null
    when 3
      headers         = options[ 'headers'  ] ? false
      seed            = options[ 'seed'     ] ? null
    else
      throw new Error "expected 2 or 3 arguments, got #{arity}"
  #.........................................................................................................
  if n <= 0 or n != Math.floor n
    throw new Error "expected a positive non-zero integer, got #{n}"
  #.........................................................................................................
  idx       = -1
  m         = 0
  p         = 1
  rnd_pick  = rnd_from_seed seed
  rnd_idx   = rnd_from_seed seed
  collector = []
  #.........................................................................................................
  on_data = ( record ) ->
    ### thx to http://math.stackexchange.com/a/890284/168522,
    http://math.stackexchange.com/a/890285/168522 ###
    idx += 1
    #.......................................................................................................
    unless idx is 0 and headers
      m += 1
      if m <= n
        collector.push record
      else
        p = n / m
        if rnd_pick() < p
          collector[ get_random_integer rnd_idx, 0, n - 1 ] = record
    #.......................................................................................................
    @emit 'data', record
  #.........................................................................................................
  on_end = ->
    result_handler null, collector
    @emit 'end'
  #.........................................................................................................
  return ES.through on_data, on_end

#-----------------------------------------------------------------------------------------------------------
@$collect = ( handler = null ) ->
  collector = []
  return @remit ( record, send, end ) =>
    collector.push record if record?
    if end?
      if handler? then handler null, collector else send collector
      end()


#===========================================================================================================
# NGRAMS
#-----------------------------------------------------------------------------------------------------------
@$ngrams = ( min, max ) ->
  ### Given two length boundaries `min` and `max`, return a transformer that, when applied to any piece of
  data that has a `Array`-ish accessor model (i.e. a `length` attribute and numeric elements), will
  pass on a list `[ record, ngrams, ]`, where `record` is the original data and `ngrams` is a list of
  all slices of the record with adjacent elements ranging from length `min` to length `max`.  ###
  return @remit ( record, send ) =>
    send [ record, @ngrams record, min, max ] if record?

#-----------------------------------------------------------------------------------------------------------
@ngrams = ( x, min, max ) ->
  unless ( count = x.length )?
    throw new Error "unable to find ngrams for value of type #{TYPES.type_of x}"
  #.........................................................................................................
  R         = []
  last_idx  = count - 1
  if count >= min
    for d in [ min .. max ]
      if ( stop = count - d ) >= 0
        for idx_0 in [ 0 .. count - d ]
          R.push x[ idx_0 ... idx_0 + d ]
  return R


#===========================================================================================================
# STREAM END DETECTION
#-----------------------------------------------------------------------------------------------------------
@$signal_end = ( signal = @eos ) ->
  ### Given an optional `signal` (which defaults to `null`), return a stream transformer that emits
  `signal` as last value in the stream. Observe that whatever value you choose for `signal`, that value
  should be gracefully handled by any transformers that follow in the pipe. ###
  on_data = null
  on_end  = ->
    @emit 'data', signal
    @emit 'end'
  return ES.through on_data, on_end

#-----------------------------------------------------------------------------------------------------------
@$on_end    = ( method ) -> @$_on_end method, no
@$catch_end = ( method ) -> @$_on_end method, yes

#-----------------------------------------------------------------------------------------------------------
@$_on_end = ( method, do_catch = no ) ->
  return @remit ( data, send, end ) ->
    send data if data?
    if end?
      method send, end
      end() unless do_catch

#-----------------------------------------------------------------------------------------------------------
@$on_start = ( method ) ->
  is_first = yes
  return @remit ( data, send ) ->
    method send if is_first
    is_first = no
    send data

#===========================================================================================================
# OBJECT CONVERSION
#-----------------------------------------------------------------------------------------------------------
@$value_from_key = ( x ) ->
  return @remit ( key, send ) ->
    send x[ key ] if x?

#-----------------------------------------------------------------------------------------------------------
@$read_values = ( x ) ->
  ### TAINT produces an intermediate list of object keys ###
  return @$chain ( @read_list Object.keys x ), ( @$value_from_key x )

# #-----------------------------------------------------------------------------------------------------------
# @read_keys = ( x ) ->
#   return @as_readable ( count, handler ) ->
#     for key of x
#       handler null, key
#     @emit 'end'

# #-----------------------------------------------------------------------------------------------------------
# @read_values = ( x ) ->
#   return @as_readable ( count, handler ) ->
#     for _, value of x
#       handler null, value
#     @emit 'end'

# #-----------------------------------------------------------------------------------------------------------
# @read_facets = ( x ) ->
#   return @as_readable ( count, handler ) ->
#     for key, value of x
#       handler null, [ key, value, ]
#     @emit 'end'



#===========================================================================================================
# DISPLAY / REPORTING
#-----------------------------------------------------------------------------------------------------------
@$show = ( badge = null ) ->
  my_show = TRM.get_logger 'info', badge ? '*'
  return @remit ( record, send ) =>
    my_show rpr record
    send record

#-----------------------------------------------------------------------------------------------------------
@$count = ( handler ) ->
  ### TAINT make signal configurable ###
  ### Given an optional `handler`, issue a count of records upon stream completion.

  When a handler is given, then it is called the NodeJS way as `handler null, count`; all records are passed
  through untouched and may be consumed by downstream transformers.

  Conversely, when no handler is given, then `$count` acts as an aggregator: all records are silently
  tossed, and the only item left in the pipe is the count.

  In any event, if a PipeDreams End-Of-Stream value is detected in the pipe, it is passed through and not
  counted; when `$count` is called without a handler, then the EOS will be sent *after* the count. ###
  eos           = @eos
  received_eos  = false
  count         = 0
  #.........................................................................................................
  if handler?
    #.......................................................................................................
    on_data = ( record ) ->
      if record is eos
        received_eos = true
      else
        count += 1
        @emit 'data', record
    #.......................................................................................................
    on_end = ->
      handler null, count
      @emit 'data', eos if received_eos
      @emit 'end'
  #.........................................................................................................
  else
    #.......................................................................................................
    on_data = ( record ) ->
      if record is eos
        received_eos = true
      else
        count += 1
    #.......................................................................................................
    on_end = ->
      @emit 'data', count
      @emit 'data', eos if received_eos
      @emit 'end'
  #.........................................................................................................
  return ES.through on_data, on_end


#===========================================================================================================
# CSV
#-----------------------------------------------------------------------------------------------------------
@$parse_csv = ( options ) ->
  field_names = null
  options    ?= {}
  headers     = options[ 'headers'    ] ? true
  delimiter   = options[ 'delimiter'  ] ? ','
  qualifier   = options[ 'qualifier'  ] ? '"'
  #.........................................................................................................
  return @remit ( record, send ) =>
    if record?
      values = ( S record ).parseCSV delimiter, qualifier, '\\'
      if headers
        if field_names is null
          field_names = values
        else
          record = {}
          record[ field_names[ idx ] ] = value for value, idx in values
          send record
      else
        send values


#===========================================================================================================
# PODs
#-----------------------------------------------------------------------------------------------------------
@$as_pods = ->
  record_idx  = -1
  field_names = null
  #.........................................................................................................
  return @remit ( record, send ) =>
    if record?
      if ( record_idx += 1 ) is 0
        field_names = record
      else
        R = {}
        for field_value, field_idx in record
          field_name      = field_names[ field_idx ]
          R[ field_name ] = field_value
        send R

#-----------------------------------------------------------------------------------------------------------
@$delete_prefix = ( prefix ) ->
  if njs_util.isRegExp prefix then  starts_with = ( text, prefix ) -> prefix.test text
  else                              starts_with = ( text, prefix ) -> ( text.lastIndexOf prefix, 0 ) is 0
  #.........................................................................................................
  return @remit ( record, send ) =>
    if record?
      for old_field_name, field_value of record
        continue unless starts_with old_field_name, prefix
        new_field_name =  old_field_name.replace prefix, ''
        continue if new_field_name.length is 0
        ### TAINT should throw error ###
        continue if record[ new_field_name ]?
        record[ new_field_name ] = field_value
        delete record[ old_field_name ]
      send record

#-----------------------------------------------------------------------------------------------------------
@$dasherize_field_names = ->
  return @remit ( record, send ) =>
    if record?
      for old_field_name of record
        new_field_name = old_field_name.replace /_/g, '-'
        continue if new_field_name is old_field_name
        @_rename record, old_field_name, new_field_name
      #.......................................................................................................
      send record

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
  return @remit ( record, send ) =>
    if record?
      send @_rename record, old_field_name, new_field_name

#-----------------------------------------------------------------------------------------------------------
@$copy = ( old_field_name, new_field_name ) ->
  return @remit ( record, send ) =>
    if record?
      send @_copy record, old_field_name, new_field_name, 'copy'

#-----------------------------------------------------------------------------------------------------------
@$set = ( field_name, field_value ) ->
  return @remit ( record, send ) =>
    if record?
      record[ field_name ] = field_value
      send record

#-----------------------------------------------------------------------------------------------------------
@$pick = ( field_names..., options ) ->
  if TYPES.isa_text options
    options = {}
    field_names.push options
  throw new Error "need at least one field name" unless field_names.length > 0
  fallback  = options[ 'fallback' ]
  send_list = field_names.length > 1
  return @remit ( record, send ) =>
    if record?
      Z = []
      for field_name in field_names
        value = record[ field_name ]
        if value is undefined
          if fallback is undefined
            return send.error new Error "field #{rpr field_name} not defined in #{rpr record}"
          value = fallback
        Z.push value
      send if send_list then Z else Z[ 0 ]

#-----------------------------------------------------------------------------------------------------------
@$insert = ( values... ) ->
  return @remit ( record, send ) =>
    record.unshift values[ idx ] for idx in [ values.length - 1 .. 0 ] by -1
    send record

#-----------------------------------------------------------------------------------------------------------
@$transform = ( transformer ) ->
  return @remit ( record, send ) =>
    send new_record unless ( new_record = transformer record ) is undefined

#-----------------------------------------------------------------------------------------------------------
@$transform_field = ( field_name, transformer ) ->
  ### Given a `field_name` and a `transformer`, (return a pipeable function with this behavior:) apply
  `transformer` to each record as `record[ field_name ] = transformer record[ field_name ]`. However,
  when `transformer` returns `undefined`, then the field is removed from record (using `Array.splice` if
  `record` is a list). If `record[ field_name ]` happens to be undefined *before* `transformer` is called,
  and error is passed on instead. ###
  return @remit ( record, send ) =>
    value = record[ field_name ]
    return send.error new Error "field #{rpr field_name} not defined in #{rpr record}" if value is undefined
    #.......................................................................................................
    if ( new_value = transformer value ) is undefined
      if TYPES.isa_list record
        record.splice field_name, 1
      else
        delete record[ field_name ]
    else
      record[ field_name ] = new_value
    #.......................................................................................................
    send record


#===========================================================================================================
# HELPERS
#-----------------------------------------------------------------------------------------------------------
get_random_integer = ( rnd, min, max ) ->
  return ( Math.floor rnd() * ( max + 1 - min ) ) + min

#-----------------------------------------------------------------------------------------------------------
rnd_from_seed = ( seed ) ->
  return if seed? then ( require 'coffeenode-bitsnpieces' ).get_rnd seed else Math.random
