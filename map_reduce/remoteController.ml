open Async.Std

module type Queue = sig 
  (** An asynchronous queue. 'a s can be pushed onto the queue; popping blocks
    until the queue is non-empty and then returns an element. *)
  type 'a t

  (** Create a new queue *)
  val create : unit -> 'a t

  (** Add an element to the queue. *)
  val push   : 'a t -> 'a -> unit

  (** Wait until an element becomes available, and then return it.  *)
  val pop    : 'a t -> 'a Deferred.t
end

module AQUEUE : Queue = struct 
  type 'a t = ('a Pipe.Reader.t * 'a Pipe.Writer.t) 

  let create () = Pipe.create ()

  let push q x = ignore (match q with 
              | (r,w) -> Pipe.write w x) 

  let pop  q = 
    let (r,w) = q in Pipe.read r >>= fun v -> 
      match v with 
      | `Eof -> failwith "empty queue"
      | `Ok x -> return x  
end 

let q = AQUEUE.create () 

let init addrs =
  List.fold_left (fun acc e -> let (s,i) = e in 
    AQUEUE.push q ((Tcp.to_host_and_port s i), i) )  () addrs 

exception InfrastructureFailure
exception MapFailure of string
exception ReduceFailure of string

module Make (Job : MapReduce.Job) = struct
  (* Remember that map_reduce should be parallelized. Note that [Deferred.List]
     functions are run sequentially by default. To achieve parallelism, you may
     find the data structures and functions you implemented in the warmup
     useful. Or, you can pass [~how:`Parallel] as an argument to the
     [Deferred.List] functions.*)
  let map_reduce inputs = failwith "lol look at comments below"

    (*calling init on each connection and telling it what job it's doing*)
   (*  List.fold_left (fun acc e -> let (connection, port) = AQUEUE.pop q in 
      Tcp.connect connection >>= fun v1 -> let (s,r,w) = v1 in 
      Worker.init port >>= fun _ -> Pipe.write w Job.name

    ) 
    (return []) inputs  *)
    (*end*)


    
end

