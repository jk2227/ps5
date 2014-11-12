open Async.Std
open AQueue

let q = AQueue.create () 
let initLst = ref [] 
let init addrs = initLst := addrs; ()

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

let workersLeft = ref (List.length !initLst)

  let map_reduce inputs = 
    let rec map input = 
        AQueue.pop q >>= fun c -> let (s,r,w) = c in 
        WRequest.send w (WRequest.MapRequest input);
        WResponse.receive r >>= fun res ->  
      begin match res with
      | `Eof -> workersLeft := !workersLeft - 1; 
        if !workersLeft == 0 then raise InfrastructureFailure
        else map input (*when worker fails*) 
      | `Ok (WResponse.JobFailed x) -> raise (MapFailure x) 
      | `Ok (WResponse.MapResult lst) -> AQueue.push q c; return lst 
      | `Ok (WResponse.ReduceResult o) -> failwith "Impossible to return a 
                                                    ReduceResult"
    end in  

    let rec reduce (k ,vl) =
        AQueue.pop q >>= fun c -> let (s,r,w) = c in 
        WRequest.send w (WRequest.ReduceRequest (k, vl));
        WResponse.receive r >>= fun res -> 
      begin match res with
      | `Eof -> workersLeft := !workersLeft - 1;
        if !workersLeft == 0 then raise InfrastructureFailure
        else reduce (k, vl) (*when worker fails*)
      | `Ok (WResponse.JobFailed x) -> raise (ReduceFailure x)
      | `Ok (WResponse.MapResult lst) -> failwith "Impossible to return a
                                                   MapResult"
      | `Ok (WResponse.ReduceResult o) -> AQueue.push q c; return (k, o)
    end in

    let rec terminate () = 
      if !workersLeft = 0 then ()
      else begin
      ignore (AQueue.pop q >>= fun (s,r,w) -> workersLeft := !workersLeft -1; 
        return (Socket.shutdown s `Both); 
      ); terminate ()
    end
  in

    let start ()= 
      let lst = !initLst in 
      List.fold_left(fun acc e -> 
        let (s,i) = e in
        ignore( Tcp.connect (Tcp.to_host_and_port s i) >>= fun v -> 
          let (s,r,w) = v in 
          Writer.write_line w Job.name; 
          AQueue.push q v; (return ()) )) () (lst) 
    
    
  in

    start (); 
    Deferred.List.map ~how: `Parallel inputs ~f: map 
    >>| List.flatten
    >>| C.combine
    >>= Deferred.List.map ~how: `Parallel ~f: reduce
    >>= fun x -> terminate (); 
                 (*close everything*) return x
end