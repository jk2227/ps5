open Async.Std

let fork d f1 f2 =
  ignore(d >>= fun v1 -> f1 v1);
  ignore(d >>= fun v2 -> f2 v2)

let deferred_map l f =
  let temp =  List.map f l in 
    let rev = List.fold_left 
      (fun acc e -> e >>= fun v1 -> acc >>= fun v2 -> return (v1::v2)) 
      (return []) temp in 
  rev >>= fun v1 -> return (List.rev v1)  
