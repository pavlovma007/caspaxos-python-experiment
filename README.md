# TODO
* [x] asincio in client api
* [x] Acceptor может обрабатывать не только свои запросы. 
    partition вшит в него не по отдельности а как диапазон
* [x] какие из акцепторов для каких partitions ?
* [x] пропрозер не должен выполнять новый запрос про ключу 
  если в процессе что то есть без ответа.
  **как делать?** 
* [ ] korni3 проверить , если быстро запускать процессы вставки подряд то иногда не вставляется - надо подождать доступа к бд
* [ ] пропозеру брать запросы только то, что запросы мне непосредственно, а то все пропозеры партиции схватятзя за запрос
* [ ] клиент отправляет запрос конкретному proposer у
* [ ] aссeptor доверяет не любым пропозерам
* [ ] client отправляет запрос одному довереному пропозеру



# casPaxos

This is a prototype implementation of the [CAS paxos consensus algorith](https://github.com/rystsov/caspaxos/blob/master/latex/caspaxos.pdf)                        

This prototype was just to put down my idea in code;                                      
I am now implememnting the real thing over there -> https://github.com/komuw/kshaka                    


предусмотреть способ чтобы пропозер не брал новых запросов а остановился 