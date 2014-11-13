open Async.Std

(*takes a deferred computation and two functions, and runs the two functions 
concurrently when the deferred computation becomes determined, passing those
functions the value determined by the original deferred:*)
(*requires: a deferred computation d of type 'a Deferred.t, and two functions 
f1 and f2 that both take in an 'a and yields a 'b Deferred.t and 'c Deferred.t 
respectively*)
(*returns: a unit; the result of f1 and f2 applied to the determined
value is ignored*)
let fork d f1 f2 =
  ignore(d >>= fun v1 -> f1 v1);
  ignore(d >>= fun v2 -> f2 v2)

(*Ignoring concurrency, deferred_map should have the same input–output 
behavior as List.map.That is, both take a list l, a function f, and return the 
result of mapping the list throughthe function. But deferred_map should apply f
concurrently—not sequentially—to each element of l.*)
(*requires: an 'a list l and a function f of type 'a -> 'b Deferred.t *)
(*returns: a 'b list Deferred.t that has applied f to each element of 
the input list l *)
let deferred_map l f =
  let temp =  List.map f l in 
    let rev = List.fold_left 
      (fun acc e -> e >>= fun v1 -> acc >>= fun v2 -> return (v1::v2)) 
      (return []) temp in 
  rev >>= fun v1 -> return (List.rev v1)  
