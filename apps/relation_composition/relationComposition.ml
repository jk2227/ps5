open Async.Std
open AppUtils

type relation = R | S

(******************************************************************************)
(** {RelationComposition Job}                                                 *)
(******************************************************************************)

module Job = struct
  type input  = relation * string * string
  type key    = string 
  type inter  = relation * string 
  type output = (string * string) list

  let name = "composition.job"

  (** Keys the relation on the element in the middle set
      Intermediate value on other element designated as from set 1 or 3 *)
  let map (r, x, y) =
    return (match r with
    | R -> [(y, (r, x))]
    | S -> [(x, (r, y))])

  let reduce (k, vs) =
  (** Splits the list to a list of elements from set 1 and one from set 3 *)
    let splitter lst = 
      List.fold_left (fun (a1, a2) (typ, dat) -> 
        match typ with
        | R -> (dat::a1, a2)
        | S -> (a1, dat::a2)
      )
      ([], []) lst
    in
    (** Generates list of pairs of elements from a list 1 by elements 
    from a list 2 *)
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

(******************************************************************************)
(** {RelationComposition App}                                                 *)
(******************************************************************************)

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
      | _ -> failwith "Incorrect number of input files."
  end
end

let () = MapReduce.register_app (module App)
