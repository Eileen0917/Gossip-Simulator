#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
// open System.Collections.Generic
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

// global variables
[<AutoOpen>]
module Globals =
    let nodes = System.Collections.Generic.Dictionary<int,List<int>>()
    let count = System.Collections.Generic.Dictionary<int,int>()
// let mutable nodes = Map.empty<int, List<int>>
// let mutable count = Map.empty<int, int>
let mutable globalTime: int64 = int64 0

// functions of topology
let nodesFull(n: int) =
    printfn "full"
    for i in 0 .. (n - 1) do
        count.Add(i, 0)
        let mutable ls = List.empty<int>
        // let mutable ls = new List<int>()
        for j in 0 .. (n - 1) do
            if i <> j then 
                ls <- j :: ls
        nodes.Add(i, ls)

let nodes2d (n: int) = 
    printfn "2d"

    let closeSqrt: int = int(ceil(sqrt(double n)))
    for i in 0 .. (n - 1) do
        count.Add(i, 0)
        let mutable ls = List.empty<int>

        if (i - closeSqrt >= 0) then ls <- (i - closeSqrt) :: ls
        if (closeSqrt + i <= n - 1) then ls <- (closeSqrt + i) :: ls
        if (i % closeSqrt <> 0) then ls <- (i - 1) :: ls
        if ( (i + 1) % closeSqrt <> 0 && (i + 1) < n) then ls <- (i + 1) :: ls
        
        nodes.Add(i, ls)
    
let nodesLine (n: int) = 
    printfn "line"
    for i in 0 .. (n - 1) do
        count.Add(i, 0)
        if i = 0 then 
            nodes.Add(i, [i + 1;])
        elif (i = n - 1) then nodes.Add(i, [i - 1;])
        else nodes.Add(i, [ i + 1; i - 1; ])

let nodesImp2d (n: int) = 
    printfn "imp 2d la"

    let closeSqrt: int = int(ceil(sqrt(double n)))

    for i in 0 .. (n - 1) do
        count.Add(i, 0)
        let mutable ls = List.empty<int>
        if (i - closeSqrt >= 0) then ls <- (i - closeSqrt) :: ls
        if (closeSqrt + i <= n - 1) then ls <- (closeSqrt + i) :: ls
        if (i % closeSqrt <> 0) then ls <- (i - 1) :: ls
        if ( (i + 1) % closeSqrt <> 0 && (i + 1) < n) then ls <- (i + 1) :: ls

        let mutable macG = Random().Next(n)

        while List.contains macG ls do
            macG <- Random().Next(n)

        ls <- macG :: ls
        nodes.Add(i, ls)

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
                    | _ -> bossMailbox.Sender() <! "Wrong Topology Type"
                    
                    for (i <- 0 to numNodes - 1) {
                        var len: Int = show(global.map.get(i)).asInstanceOf[Int];
                        z(i) = system1.actorOf(Props(new Node(i, len, self)), name = "Node" + i);
                      }

                    match a with
                    | "gossip" -> bossMailbox.Sender() <! "gossip la"
                    | "push-sum" -> bossMailbox.Sender() <! "push-sum la"
                    | _ -> bossMailbox.Sender() <! "Wrong Algorithm Type"

                    
                | DONE -> 
                    count <- count - 1
                    if count = 0 then 
                        printfn "DONE"
                        Environment.Exit 1
                
                return! bossLoop()
            }
        bossLoop()

// let main() =
//     let args = System.Environment.GetCommandLineArgs()
//     let numsOfNodes = int args.[3]
//     let topology = string args.[4]
//     let alg = string args.[5]
//     for timeout in [1000000] do
//         try
//             let task = (boss <? START (numsOfNodes, topology, alg))
//             Async.RunSynchronously (task, timeout)

//         with :? TimeoutException ->
//             printfn "ask: timeout!"
//     0

// main()

async {
    let args = System.Environment.GetCommandLineArgs()
    let numsOfNodes = int args.[3]
    let topology = string args.[4]
    let alg = string args.[5]
    let! response = boss <? START (numsOfNodes, topology, alg)
    printfn "%s" response
} |> Async.RunSynchronously