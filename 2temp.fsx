#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open System.Collections.Generic
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

type BossMessage = 
    | START of int * string * string
    | GDONE
    | PDONE of string * int * double

type NodeType = 
    | GOSSIP of string
    | PUSHSUM of double * double

// let system = System.create "system" (Configuration.defaultConfig())
let system = ActorSystem.Create("FSharp")

// global variables
// [<AutoOpen>]
// module Globals =
//     let nodes = System.Collections.Generic.Dictionary<int,List<int>>()
//     let mutable globalCount = System.Collections.Generic.Dictionary<int,int>()
let mutable nodes = Map.empty<int, list<int>>
let mutable globalCount = Map.empty<int, int>
let mutable globalTime: int64 = int64 0

// functions of topology
let nodesFull(n: int) =
    printfn "full"
    for i in 0 .. (n - 1) do
        globalCount.Add(i, 0)
        let mutable ls = List.empty<int>
        for j in 0 .. (n - 1) do
            if i <> j then 
                ls <- j :: ls
        nodes.Add(i, ls)

let nodes2d (n: int) = 
    printfn "2d"

    let closeSqrt: int = int(ceil(sqrt(double n)))
    for i in 0 .. (n - 1) do
        globalCount.Add(i, 0)
        let mutable ls = List.empty<int>

        if (i - closeSqrt >= 0) then ls <- (i - closeSqrt) :: ls
        if (closeSqrt + i <= n - 1) then ls <- (closeSqrt + i) :: ls
        if (i % closeSqrt <> 0) then ls <- (i - 1) :: ls
        if ( (i + 1) % closeSqrt <> 0 && (i + 1) < n) then ls <- (i + 1) :: ls
        
        nodes.Add(i, ls)
    
let nodesLine (n: int) = 
    printfn "line"
    for i in 0 .. (n - 1) do
        globalCount.Add(i, 0)
        if i = 0 then 
            nodes.Add(i, [i + 1;])
        elif (i = n - 1) then nodes.Add(i, [i - 1;])
        else nodes.Add(i, [ i + 1; i - 1; ])

let nodesImp2d (n: int) = 
    printfn "imp 2d la"

    let closeSqrt: int = int(ceil(sqrt(double n)))

    for i in 0 .. (n - 1) do
        globalCount.Add(i, 0)
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
        let mutable flag = 0
        let mutable s = n
        let mutable w = 1
        let slidingWindow = Queue.empty<Double>
        slidingWindow.Enqueue(10000)
        slidingWindow.Enqueue(10000)

        match msg with
        | GOSSIP (str, len, n) ->
            printfn "node gg"
            // let rumor: string = msg.[0]                               
            // nodeMailbox.Sender() <! DONE rumor
            if count < 10 then 
                count <- count + 1
                globalCount.[n] <- globalCount.[n] + 1

                if count = 10 then
                    boss <! GDONE
                
                let random:int = Random().Next(len)
                let nei:int = nodes.[n].[random]

                // let mixPath s = "../Node" + nei
                select ("../Node" + nei) system <! "Hello"
                select ("../Node" + n) system <! "Hello"
            else
                sender <! GIDONE (len, n)

        | PUSHSUM (sum, weight) ->
            printfn "node ps"
            // let sum: double = msg.[0]
            // let weight: double = msg.[1]
            // nodeMailbox.Sender() <! DONE sum
            if flag = 0 then
                s <- s + sum
                w <- w + weight
                let ratio = (s / w)
                s <- s / 2
                w <- w / 2
                slidingWindow.Enqueue(ratio)

                if (abs(slidingWindow.head - slidingWindow.last) <= 0.001) then
                    flag <- 1
                    nodeMailbox.Sender() <! (PDONE ("done", n, ratio))
                

                slidingWindow.Dequeue()
                let ran1:int = Random().Next(len)
                let nei1:int = nodes.[n].[ran1]

                select ("../Node" + nei) system <! (s, w)
                select ("../Node" + n) system <! (s, w)
            else
                sender <! PIDONE (sum, weight, len, n)

        | GIDONE (len, n) ->
            let random:int = Random().Next(len)
            let nei:int = nodes.[n].[random]
            select ("../Node" + nei) system <! "Hello"

        | PIDONE (sum1, weight1, len, n) ->
            let random1:int = Random().Next(len)
            let nei1:int = nodes.[n].[random1]
            select ("../Node" + nei1) system <! (sum1, weight1);

        return! loop ()
    }
    loop ()

let boss = 
    spawn system "boss" 
    <| fun bossMailbox ->
        let rec bossLoop() =
            actor {
                let! (msg: BossMessage) = bossMailbox.Receive()
                let mutable n = 0
                let mutable keepTrack = Array.create n 0.0
                let mutable count = 0
                match msg with
                | START (n, t, a) ->
                    let mutable x = Array.init n

                    match t with 
                    | "full" -> nodesFull(n)
                    | "2D" -> nodes2d(n)
                    | "line" -> nodesLine(n)
                    | "imp2D" -> nodesImp2d(n)
                    | _ -> bossMailbox.Sender() <! "Wrong Topology Type"
                    
                    
                    for i in 0 .. (n-1) do
                        let mutable nodeName = "node"
                        nodeName <- nodeName + i                        
                        x.[i] = spawn system nodeName node

                    globalTime <- Environment.TickCount
                    let len = List.length nodes.[0]
                    
                    match a with
                    | "gossip" -> x.[0] <! ("Hello", len, n)
                    | "push-sum" -> x.[0] <! (0.0,1.0, len, n)
                    | _ -> bossMailbox.Sender() <! "Wrong Algorithm Type"

                    
                | GDONE -> 
                    // bossMailbox.Sender() <! "Done"
                    // count <- count - 1
                    // if count = 0 then 
                    //     printfn "DONE"
                    //     Environment.Exit 1
                    count <- count + 1
                    if count = n then
                        printfn "GOSSIP DONE"
                        printfn "Time taken is %u" (Environment.TickCount - globalTime)
                        Environment.Exit 1
                
                | PDONE (str, id, ratio) ->
                    count <- count + 1
                    keepTrack.[id] <- ratio
                    let threshold:int = int(0.95 * n)
                    if count = n then 
                        printfn "Master Done"
                        let time = Environment.TickCount - globalTime

                        for i in 0 .. (n - 1) do
                            printfn "Node %u ratio is %u" i keepTrack.[i]
                        
                        printfn "Time token is %u" time
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

// async {
//     let args = System.Environment.GetCommandLineArgs()
//     let numsOfNodes = int args.[3]
//     let topology = string args.[4]
//     let alg = string args.[5]
//     let! response = boss <? START (numsOfNodes, topology, alg)
//     printfn "%s" response
// } |> Async.RunSynchronously