infinispan-queue
================

Queue Implementation using Infinispan Data Grid.

Infinispan (http://infinispan.org/) is an open source distributed memory Key/Value data grid solution from Red Hat/JBoss.
Solutions such as Hazelcast and redis provide a data structure such as Queue in addition to the Key/Value method.
Can be used.

In Infinispan, we implemented a sample by creating a Linked-List with only the Key / Value structure of the same data structure as Queue.
Since Infinispan supports transactions, transactions are used for input/output of pointers and queue elements.

* Infinispan Queue Slide
http://www.slideshare.net/opennaru/20130226-infinispan-queue

* Source Github 
https://github.com/nameislocus/infinispan-queue/

