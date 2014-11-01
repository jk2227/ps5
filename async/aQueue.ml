open Async.Std

type 'a t = ('a Pipe.Reader.t * 'a Pipe.Writer.t) 

let create () = Pipe.create ()

let push q x = ignore (match q with 
              | (r,w) -> Pipe.write w x) 

let pop  q = 
    let (r,w) = q in Pipe.read r >>= fun v -> 
      match v with 
      | `Eof -> failwith "empty queue"
      | `Ok x -> return x  
