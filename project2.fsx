#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open System.Collections.Generic
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

type starterMessage =
    | Start of int * string * string

type BossMessage = 
    | GossipStart
    | AddNodeToGossip
    | Gossiping
    | GossipDone


type NodeType = 


let system = ActorSystem.Create("FSharp")

// global variables
let mutable globalTime: int64 = int64 0

let buildTopo(t:string, n: int): Dictionary<int, int list> = 
    let nodes = System.Collections.Generic.Dictionary<int,int list>()

    match t with 
    | "full" -> 
        for i in 0 .. (n - 1) do
            let mutable ls = List.empty<int>
            for j in 0 .. (n - 1) do
                if i <> j then 
                    ls <- j :: ls
            nodes.Add(i, ls)

    | "2D" -> 
        let closeSqrt: int = int(ceil(sqrt(double n)))
        for i in 0 .. (n - 1) do
            let mutable ls = List.empty<int>

            if (i - closeSqrt >= 0) then ls <- (i - closeSqrt) :: ls
            if (closeSqrt + i <= n - 1) then ls <- (closeSqrt + i) :: ls
            if (i % closeSqrt <> 0) then ls <- (i - 1) :: ls
            if ( (i + 1) % closeSqrt <> 0 && (i + 1) < n) then ls <- (i + 1) :: ls
            
            nodes.Add(i, ls)
        
    | "line" -> 
        for i in 0 .. (n - 1) do
            if i = 0 then nodes.Add(i, [i + 1;])
            elif (i = n - 1) then nodes.Add(i, [i - 1;])
            else nodes.Add(i, [ i + 1; i - 1; ])
        
    | "imp2D" -> 
        let closeSqrt: int = int(ceil(sqrt(double n)))

        for i in 0 .. (n - 1) do
            let mutable ls = List.empty<int>
            if (i - closeSqrt >= 0) then ls <- (i - closeSqrt) :: ls
            if (closeSqrt + i <= n - 1) then ls <- (closeSqrt + i) :: ls
            if (i % closeSqrt <> 0) then ls <- (i - 1) :: ls
            if ( (i + 1) % closeSqrt <> 0 && (i + 1) < n) then ls <- (i + 1) :: ls

            let mutable macG = Random().Next(n)

            while List.contains macG ls || macG = i do
                macG <- Random().Next(n)

            ls <- macG :: ls
            nodes.Add(i, ls)
    | _ -> nodes.Add(0, [0])

    nodes

let node (nodeMailbox:Actor<NodeType>) = 
    let rec loop () = actor {
        let! (msg: NodeType) = nodeMailbox.Receive()
    }
    loop ()

let boss (bossMailbox:Actor<BossMessage>) = 
    let rec loop () = actor {
        let! (msg: BossMessage) = bossMailbox.Receive()

        match msg with 
        | AddGossipNode idx ->
            actNodeArr <- Array.append actNodeArr [|idx|]
    
        | Gossiping str ->
            if actNodeArr.Length <> 0 then 
                for index in actNodeArr do
                    let nodeAct = select ("akka://FSharp/user/node" + string index) system
                    nodeAct <! GossipAgain str     
            
        | GOSSIPDONE ->   
            actNodeArr <- Array.filter ((<>) index) actNodeArr 
            count <- count + 1
            if count = numNodes then
                printfn "GOSSIP DONE"
                printfn "Time taken is %u" (Environment.TickCount64 - globalTime)
                Environment.Exit 1
    }
    loop ()


let main() =
    let args = System.Environment.GetCommandLineArgs()
    let numsOfNodes = int args.[3]
    let topology = string args.[4]
    let alg = string args.[5]
    for timeout in [1000000] do
        try
            let task = (starter <? START (numsOfNodes, topology, alg))
            Async.RunSynchronously (task, timeout)

        with :? TimeoutException ->
            printfn "ask: timeout!"
    0

main()
