open Async.Std
open Async_unix

type filename = string

(******************************************************************************)
(** {2 The Inverted Index Job}                                                *)
(******************************************************************************)

module Job = struct
  type input  = string * string list
  type key    = string
  type inter  = string
  type output = string list

  let name = "index.job"

  let map input : (key * inter) list Deferred.t =
    let (file, wordlist) = input in
    return (List.fold_left (fun a e -> (e, file)::a) [] wordlist)

  let reduce (key, inters) : output Deferred.t =
    let compr s1 s2 = if (s1 < s2) then -1 else
                      if (s1 = s2) then 0 else 1
    in
    let remove_dups_sorted lst = 
      List.fold_left (fun a e -> 
        match a with
        | h::t -> if e = h then a else e::a
        | [] -> e::a
      ) [] lst
    in
    return (  remove_dups_sorted (List.sort compr inters))
end

(* register the job *)
let () = MapReduce.register_job (module Job)


(******************************************************************************)
(** {2 The Inverted Index App}                                                *)
(******************************************************************************)

module App  = struct

  let name = "index"

  (** Print out all of the documents associated with each word *)
  let output results =
    let print (word, documents) =
      print_endline (word^":");
      List.iter (fun doc -> print_endline ("    "^doc)) documents
    in

    let sorted = List.sort compare results in
    List.iter print sorted


  (** for each line f in the master list, output a pair containing the filename
      f and the contents of the file named by f.  *)
  let read (master_file : filename) : (filename * string) list Deferred.t =
    Reader.file_lines master_file >>= fun filenames ->

    Deferred.List.map filenames (fun filename ->
      Reader.file_contents filename >>| fun contents ->
      (filename, contents)
    )

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (** The input should be a single file name.  The named file should contain
        a list of files to index. *)
    let main args =
      match args with
      | [s] -> begin
        read s >>= (fun name_data_pair -> Deferred.List.map name_data_pair (fun (x, y) -> return (x, AppUtils.split_words y)) )
        >>= MR.map_reduce
        >>= (fun x -> return (output x))
      end
      | _ -> failwith "Incorrect number of input files. Please pass one master 'file of file paths' file"
  end
end

(* register the App *)
let () = MapReduce.register_app (module App)

