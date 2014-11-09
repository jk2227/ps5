open Async.Std
open AppUtils

type relation = R | S

module Job = struct
  type input  = relation * string * string
  type key    = string (* TODO: choose an appropriate type *)
  type inter  = relation * string (* TODO: choose an appropriate type *)
  type output = (string * string) list

  let name = "composition.job"

  let map (r, x, y) =
    return (match r with
    | R -> [(y, (r, x))]
    | S -> [(x, (r, y))])

  let reduce (k, vs) =
    let splitter lst = 
      List.fold_left (fun (a1, a2) (typ, dat) -> 
        match typ with
        | R -> (dat::a1, a2)
        | S -> (a1, dat::a2)
      )
      ([], []) lst
    in
    let cross_prod (l1, l2) = 
      List.fold_left (fun a x ->
        List.fold_left (fun b y ->
          (x, y)::b
        ) a l2
      ) [] l1
    in
    return (cross_prod (splitter vs))
end

let () = MapReduce.register_job (module Job)

let read_line (line: string) : (string * string) =
  match Str.split (Str.regexp ",") line with
    | [domain; range] -> (String.trim domain, String.trim range)
    | _ -> failwith "Malformed input in relation file."

let read_file (r: relation) (file: string) : (relation * string * string) list Deferred.t =
      Reader.file_lines file            >>= fun lines  ->
      return (List.map read_line lines) >>= fun result ->
      return (List.map (fun (domain, range) -> (r, domain, range)) result)

module App = struct
  let name = "composition"

  let clean_and_print vs =
    List.map snd vs   |>
    List.flatten      |>
    List.sort compare |>
    List.iter (fun (a, b) -> printf "(%s, %s)\n" a b)

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (* You may assume that main is called with two valid relation files. You do
       not need to handle malformed input. For example relation files, see the
       data directory. *)
    let main args =
      match args with
      | [rfile; sfile] -> begin
          read_file R rfile >>= fun r ->
          read_file S sfile >>= fun s ->
          return (r @ s)
          >>= MR.map_reduce
          >>| clean_and_print
      end
      | _ -> failwith "Incorrect number of input files. Please provide two files."
  end
end

let () = MapReduce.register_app (module App)
