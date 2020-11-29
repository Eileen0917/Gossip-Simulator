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
    | Gossiping of string
    | AddGossipNode of int
    | GOSSIPDONE
    | PUSHSUMDONE of int * double

type NodeType = 
    | INIT of int list * int
    | GOSSIP of string
    | GossipAgain of string
    | PUSHSUM of double * double 
    | PSDONE of int * double * double

// let system = System.create "system" (Configuration.defaultConfig())
let system = ActorSystem.Create("FSharp")

// global variables
let mutable globalTime: int64 = int64 0

// functions of topology
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
    let bossActor = select ("akka://FSharp/user/boss") system
    let mutable count = 0
    let mutable nodeIndex: int = -1
    let mutable count: int = 0
    let mutable neighbors = List.empty<int>
    
    let mutable s:double = 0.0
    let mutable w:double = 1.0
    let mutable currRatio:double = 0.0
    let mutable pushCount: int = 1
    let mutable flag: int = 0

    let rec loop () = actor {
        let! (msg: NodeType) = nodeMailbox.Receive()

        match msg with
        | INIT (neis, index) ->
            neighbors <- neis @ neighbors
            nodeIndex <- index
            s <- double(neighbors.Length)
            
        | GOSSIP str ->
            if count < 10 then 
                if count = 0 then    
                    bossActor <! AddGossipNode nodeIndex

                count <- count + 1
                
                let random:int = Random().Next(neighbors.Length)
                let nei:int = neighbors.[random]
                let neighborActor = select ("akka://FSharp/user/node" + string nei) system
                neighborActor <! GOSSIP str
            else
                bossActor <! GOSSIPDONE

        | GossipAgain str ->
            if count < 10 then
                let random:int = Random().Next(neighbors.Length)
                let nei:int = neighbors.[random]
                let neighborActor = select ("akka://FSharp/user/node" + string nei) system
                neighborActor <! GOSSIP str

        | PUSHSUM (sum, weight) ->
            if flag = 0 then
                s <- s + sum
                w <- w + weight
                let mutable ratio:double = (s / w)
                s <- s / 2.0
                w <- w / 2.0

                let diff: double = abs(currRatio - ratio)

                if diff <= 10.0 ** -3.0 then
                    pushCount <- pushCount + 1
                    if pushCount = 1 then
                        flag <- 1
                        bossActor <! PUSHSUMDONE (nodeIndex, ratio)
                else
                    pushCount <- 0

                currRatio <- ratio

                let random:int = Random().Next(neighbors.Length)
                let nei:int = neighbors.[random]
                let neighborActor = select ("akka://FSharp/user/node" + string nei) system
                let selfActor = select ("akka://FSharp/user/node" + string nodeIndex) system
                neighborActor <! PUSHSUM (s, w)
                selfActor <! PUSHSUM (s, w)

            else
                let selfActor = select ("akka://FSharp/user/node" + string nodeIndex) system
                selfActor <! PSDONE (nodeIndex, sum, weight)

        | PSDONE (idx, sum, weight) ->
            let random:int = Random().Next(neighbors.Length)
            let nei:int = neighbors.[random]
            let neighborActor = select ("akka://FSharp/user/node" + string nei) system
            neighborActor <! PUSHSUM (sum, weight)

        return! loop ()
    }
    loop ()

let boss = 
    spawn system "boss" 
    <| fun bossMailbox ->
        let mutable count = 0
        let mutable numNodes = 0
        let mutable keepTrack = System.Collections.Generic.Dictionary<int,double>()

        let mutable lastTimestamp = Environment.TickCount64
        let gossipPeriod = 100
        let mutable actNodeArr: int [] = Array.empty

        let rec bossLoop() =
            actor {
                let! (msg: BossMessage) = bossMailbox.Receive()

                match msg with
                | START (n, t, a) ->
                    numNodes <- n
                    let mutable nodeActArr = Array.init n (fun i -> spawn system ("node"+ string(i)) node)
                    let topoMap = buildTopo(t, n)

                    globalTime <- Environment.TickCount64
                    for i = 0 to (numNodes - 1) do
                        // printfn "neis %i %A" i topoMap.[i]
                        nodeActArr.[i] <! INIT (topoMap.[i], i)
                    
                    match a with
                    | "gossip" -> 
                        nodeActArr.[0] <! GOSSIP "Hello!"
                        lastTimestamp <- Environment.TickCount64
                        while true do
                            if (Environment.TickCount64 - lastTimestamp) >= int64(gossipPeriod) then
                                lastTimestamp <- Environment.TickCount64
                                gossipBoss <! Gossiping "Fire!"
                                
                    | "push-sum" -> nodeActArr.[0] <! PUSHSUM (0.0,1.0)
                    | _ -> bossMailbox.Sender() <! "Wrong Algorithm Type"

                | AddGossipNode idx ->
                    actNodeArr <- Array.append actNodeArr [|idx|]
           
                | Gossiping str ->
                    if actNodeArr.Length <> 0 then 
                        for index in actNodeArr do
                            let nodeAct = select ("akka://Gossip/user/node" + string index) system
                            nodeAct <! GossipAgain str     
                    
                | GOSSIPDONE index->   
                    actNodeArr <- Array.filter ((<>) index) actNodeArr 
                    count <- count + 1
<<<<<<<< HEAD:project2.fsx
                    // printfn "count %i" count
========
>>>>>>>> 88d3424:2temp.fsx
                    if count = numNodes then
                        printfn "GOSSIP DONE"
                        printfn "Time taken is %u" (Environment.TickCount64 - globalTime)
                        Environment.Exit 1
                
                | PUSHSUMDONE (id, ratio) ->
                    count <- count + 1
                    keepTrack.Add(id, ratio)
                    if count = numNodes then 
                        printfn "Master Done"
                        let time = Environment.TickCount64 - globalTime

                        for i in 0 .. (numNodes - 1) do
                            printfn "Node %i ratio is %f" i keepTrack.[i]
                        
                        printfn "Time taken is %u" time
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
