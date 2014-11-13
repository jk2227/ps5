open Async.Std

module Make (Job : MapReduce.Job) = struct
  module WorkerRequest = Protocol.WorkerRequest (Job)
  module WorkerResponse = Protocol.WorkerResponse (Job)
  (* see .mli *)
  let run r w = 
    let rec loop r' w' =
      WorkerRequest.receive r' >>= function
      | `Eof -> return ()
      | `Ok WorkerRequest.MapRequest input -> 
          Job.map input >>= (fun result -> 
            return (WorkerResponse.send w' (WorkerResponse.MapResult result))) 
              >>= (fun _ -> loop r' w')
      | `Ok WorkerRequest.ReduceRequest (key, lst) -> 
          Job.reduce (key, lst) >>= (fun result -> 
            return (
              WorkerResponse.send w' (WorkerResponse.ReduceResult result))) 
                >>= (fun _ -> loop r' w')
  in loop r w

end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->
      Reader.read_line r >>= function
        | `Eof    -> return ()
        | `Ok job -> match MapReduce.get_job job with
          | None -> return ()
          | Some j ->
            let module Job = (val j) in
            let module Worker = Make(Job) in
            Worker.run r w
    )
    >>= fun _ ->
  print_endline "server started";
  print_endline "worker started.";
  print_endline "registered jobs:";
  List.iter print_endline (MapReduce.list_jobs ());
  never ()


