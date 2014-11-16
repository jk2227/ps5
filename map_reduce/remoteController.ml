open Async.Std
open AQueue

(*Queue that stores workers*)
let q = AQueue.create () 
(*initial list of host,port pairs that we will attempt to connect to
during the map_reduce phase*)
let initLst = ref []
(*number of functional workers*)
let workersLeft = ref 0
(*prepares connection*) 
let init addrs = initLst := addrs; workersLeft := (List.length addrs); ()

exception InfrastructureFailure
exception MapFailure of string
exception ReduceFailure of string

module Make (Job : MapReduce.Job) = struct
  (* Remember that map_reduce should be parallelized. Note that [Deferred.List]
     functions are run sequentially by default. To achieve parallelism, you may
     find the data structures and functions you implemented in the warmup
     useful. Or, you can pass [~how:`Parallel] as an argument to the
     [Deferred.List] functions.*)
  module WRequest = Protocol.WorkerRequest(Job)
  module WResponse = Protocol.WorkerResponse(Job)
  module C = Combiner.Make(Job) 

  let map_reduce inputs = 
    (*Helper function. In the case that a worker dies or misbehaves, we 
      close the connection, decrement the number of workers that are 
      functional and pass on the current job to another worker. If the number 
      of functional workers hits zero, an InfrastructureFailure exception 
      is raised*)
    let next_worker map_or_reduce arg worker =
      workersLeft := !workersLeft - 1; 
      if !workersLeft == 0 then 
        raise InfrastructureFailure
      else
        let (s,r,w) = worker in
        return (Socket.shutdown s `Both) >>= fun () -> map_or_reduce arg in

    (*Performs map part of map_reduce. Issues one map job to a worker and waits
      for the result. Also handles the cases where the connection to the worker
      is dead or the worker returns an inappropriate response.*)
    let rec map input = 
      AQueue.pop q >>= fun c -> let (s,r,w) = c in 
      WRequest.send w (WRequest.MapRequest input);
      WResponse.receive r >>= fun res ->  
      begin match res with
      | `Eof -> next_worker map input c
      | `Ok (WResponse.JobFailed x) -> raise (MapFailure x) 
      | `Ok (WResponse.MapResult lst) -> AQueue.push q c; return lst 
      | `Ok (WResponse.ReduceResult o) -> next_worker map input c
    end in  

    (*Performs reduce part of map_reduce. Issues one reduce job to a worker and 
      waits for the result. Also handles the cases where the connection to the 
      worker is dead or the worker returns an inappropriate response.*)
    let rec reduce (k, vl) =
        AQueue.pop q >>= fun c -> let (s,r,w) = c in 
        WRequest.send w (WRequest.ReduceRequest (k, vl));
        WResponse.receive r >>= fun res -> 
      begin match res with
      | `Eof -> next_worker reduce (k, vl) c
      | `Ok (WResponse.JobFailed x) -> raise (ReduceFailure x)
      | `Ok (WResponse.MapResult lst) -> next_worker reduce (k, vl) c
      | `Ok (WResponse.ReduceResult o) -> AQueue.push q c; return (k, o)
    end in

    (*Terminates map_reduce process by closing all connections.*)
    let rec terminate countdown = 
      if countdown <= 0 then 
        return ()
      else
        AQueue.pop q >>= fun (s,r,w) -> 
        return (Socket.shutdown s `Both) >>= fun () -> terminate (countdown-1)
    in

    (*Helper function. Given a host and port, this functions tries to
      establish a connection to the worker. If the connection succeeds, then
      the worker is started. If it fails, then the number of functional workers
      is decremented by one.*)
    let connect_helper (s, i) =
      let connect () =
        Tcp.connect (Tcp.to_host_and_port s i) in
      Monitor.try_with connect >>= function
      | Core.Std.Ok (s,r,w) -> 
          Writer.write_line w Job.name;
          AQueue.push q (s, r, w); return ()
      | Core.Std.Error _ -> workersLeft := !workersLeft - 1; return () in

    (*Starts the map_reduce process by attempting to connect to each of the 
      specified ports*)
    let start () = 
      let lst = !initLst in 
      List.fold_left (fun acc e -> connect_helper e) (return ()) lst in
  
    start () >>= fun () -> 
    if !workersLeft = 0 then 
      raise InfrastructureFailure 
    else
      Deferred.List.map ~how: `Parallel inputs ~f: map 
      >>| List.flatten
      >>| C.combine
      >>= Deferred.List.map ~how: `Parallel ~f: reduce
      >>= fun x -> terminate (!workersLeft) >>= fun () -> return x

end