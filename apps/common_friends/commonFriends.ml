open Async.Std

module Job = struct
  type input  = string * string list
  type key    = string * string 
  type inter  = string 
  type output = string list

  let name = "friends.job"

  let map (name, friendlist) =
    return (List.fold_left (fun a x -> 
      let key  = (min name x, max name x) in
      (List.fold_left (fun b y -> (key, y)::b) a friendlist)
    ) [] friendlist)

  let reduce (_, friendlists) =
    let compr s1 s2 = if (s1 < s2) then -1 else
                      if (s1 = s2) then 0 else 1
    in
    let rec getAdjacentDoubles lst = match lst with
      | [] | [_] -> []
      | a::b::c -> if (a = b) then a::(getAdjacentDoubles (b::c))
                              else getAdjacentDoubles (b::c)
    in
    return (getAdjacentDoubles (List.sort compr friendlists))
end

let () = MapReduce.register_job (module Job)

let read_line (line:string) :(string * (string list)) =
  match Str.split (Str.regexp ":") line with
    | [person; friends] -> begin
      let friends = Str.split (Str.regexp ",") friends in
      let trimmed = List.map String.trim friends in
      (person, trimmed)
    end
    | _ -> failwith "Malformed input in graph file."

let read_files (files: string list) : ((string * (string list)) list) Deferred.t =
  match files with
  | []    -> failwith "No graph files provided."
  | files -> begin
    Deferred.List.map files Reader.file_lines
    >>| List.flatten
    >>| List.map read_line
  end

module App = struct
  let name = "friends"

  let print common_friends =
    let print_friends ((a, b), friends) =
      printf "(%s, %s): %s\n" a b (String.concat ", " friends)
    in
    List.iter print_friends common_friends

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (* You may assume that main is called with a single, valid graph file. You
       do not need to handle malformed input. For example graph files, see the
       data directory. *)
    let main args =
        read_files args
        >>= MR.map_reduce
        (* replace this failwith with print once you've figured out the key and
           inter types*)
        >>| print
  end
end

let () = MapReduce.register_app (module App)
