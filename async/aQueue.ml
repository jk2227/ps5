open Async.Std

(* An asynchronous queue. 'a s can be pushed onto the queue; popping blocks
until the queue is non-empty and then returns an element. *)
type 'a t = ('a Pipe.Reader.t * 'a Pipe.Writer.t) 

(* Create a new queue *)
(*requires: a unit input*)
(*returns: returns a Pipe, which consists of a Reader and writer pair*)
let create () = Pipe.create ()

(* Add an element to the queue. *)
(*requires: a Reader, Writer pair to push an element to, and an element
to push into the pair*)
(*returns: a unit; as a side effect, the element has been written into
the Writer*)
let push q x = ignore (match q with 
              | (r,w) -> Pipe.write w x) 

(* Wait until an element becomes available, and then return it. *)
(*requires: a Reader,Writer pair to pop an element from*)
(*returns: reads a value from the Reader, essentially popping the element,
and returns that element as a Deferred value*)
let pop  q = 
    let (r,w) = q in Pipe.read r >>= fun v -> 
      match v with 
      | `Eof -> failwith "bleh"
      | `Ok x -> return x  
