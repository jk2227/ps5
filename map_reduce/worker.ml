open Async.Std

module Make (Job : MapReduce.Job) = struct
  module Honwei2 = Protocol.WorkerRequest (Job)
  module Honwei3 = Protocol.WorkerResponse (Job)
  (* see .mli *)
  let run r w = 
    let rec loop r' w' =
      Honwei2.receive r' >>= function
        | `Eof -> return ()
        | `Ok Honwei2.MapRequest input ->
            Job.map input >>= (fun result -> return (Honwei3.send w' (Honwei3.MapResult result))) >>= (fun _ -> loop r' w')
        | `Ok Honwei2.ReduceRequest (key, lst) ->
            Job.reduce (key, lst) >>= (fun result -> return (Honwei3.send w' (Honwei3.ReduceResult result))) >>= (fun _ -> loop r' w')
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


