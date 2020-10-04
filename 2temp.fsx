#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

type BossMessage = 
    | START of int * string * string
    | DONE

type NodeType = 
    | GOSSIP of string
    | PUSHSUM of double * double

// let system = System.create "system" (Configuration.defaultConfig())
let system = ActorSystem.Create("FSharp")

let mutable nodes = [int, [int]]
let mutable count = 0
let nodesFull(n: int) =
    printfn "full"
    // for i in 0 .. (n - 1) do
    //     count[i] = 0
    //     let mutable ls = [int]
    //     for j in 0 .. (n - 1) do
    //         if i <> j then ls <- j +: ls
    //     nodes.[i] = ls

let nodes2d (n: int) = 
    printfn "2d"
    
let nodesLine (n: int) = 
    printfn "line"
    // for i in 0 .. (n - 1) do
    //     count.[i] = 0
    //     if i = 0 then nodes.[i] <- [i + 1]
    //     elif (i = n - 1) then nodes.[i] <- List(i - 1)
    //     else nodes.[i] <- List(i - 1, i + 1)

let nodesImp2d (n: int) = 
    printfn "imp 2d la"

let node (nodeMailbox:Actor<NodeType>) = 
    let rec loop () = actor {
        let! (msg: NodeType) = nodeMailbox.Receive()
        match msg with
        | GOSSIP msg ->
            printfn "node gg"
            // let rumor: string = msg.[0]                               
            // nodeMailbox.Sender() <! DONE rumor
        | PUSHSUM (s, w) ->
            printfn "node ps"
            // let sum: double = msg.[0]
            // let weight: double = msg.[1]
            // nodeMailbox.Sender() <! DONE sum
        return! loop ()
    }
    loop ()

let boss = 
    spawn system "boss" 
    <| fun bossMailbox ->
        let rec bossLoop() =
            actor {
                let! (msg: BossMessage) = bossMailbox.Receive()
                match msg with
                | START (n, t, a) ->
                    match t with 
                    | "full" -> nodesFull(n)
                    | "2D" -> nodes2d(n)
                    | "line" -> nodesLine(n)
                    | "imp2D" -> nodesImp2d(n)
                    | _ -> printfn "Wrong Topology Type"
                    
                    match a with
                    | "gossip" -> printfn "gossip la"
                    | "push-sum" -> printfn "push-sum la"
                    | _ -> printfn "Wrong Algorithm Type"

                    
                | DONE -> 
                    count <- count - 1
                    if count = 0 then 
                        printfn "DONE"
                        Environment.Exit 1
                
                return! bossLoop()
            }
        bossLoop()

let main() =
    let args = System.Environment.GetCommandLineArgs()
    let numsOfNodes = int args.[3]
    let topology = string args.[4]
    let alg = string args.[5]
    for timeout in [1000000] do
        try
            let task = (boss <? START (numsOfNodes, topology, alg))
            Async.RunSynchronously (task, timeout)

        with :? TimeoutException ->
            printfn "ask: timeout!"
    0

main()