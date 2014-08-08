# create-readstream.coffee


#### Functions
  
* [after](#after)
  
* [this.create\_readstream](#this.create_readstream)
  
* [get\_filesize](#get_filesize)
  







## Functions
  
### <a name="after">after(time_s, f)</a>

  
### <a name="this.create_readstream">this.create\_readstream(route, label)</a>
 Create and return a new instance of a read stream form a single route or a list of routes. In the
latter case, a combined stream using [combined-stream](https://github.com/felixge/node-combined-stream) is
constructed so that several files (presumable the result of an earlier split operation) are transparently
read like a single, huge file.

As a bonus, the module uses [node-progress](https://github.com/visionmedia/node-progress) to display a
progress bar for reading operations that last for more than a couple seconds.

<!-- As a second bonus, the module uses CoffeeNode's `TRM.listen_to_keys` method to implement a `ctrl-C,
ctrl-C`-style abort shortcut with an informative message displayed when `ctrl-C` has been hit by the user
once; this is to prevent longish read operations to be inadvertantly terminated.-->

  
### <a name="get_filesize">get\_filesize(route)</a>
Helper to compute filesize from a single route or a list of routes. 
  

  
    
  

