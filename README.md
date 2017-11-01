# HATS
HATS is an implementation of clever and simple protocol that aims at providing high availability for cloud based key value stores by building resiliency against a single data center unavailability. While the protocol itself can be implemented for any key value store that provides an expected interface, this particular implementation targets Azure Table Storage.

Please look at HATS.docx for 
1) Details of the protocol 
2) API usage

This software was originally developed during November 2014. At that time there weren't good solutions for the problem discussed above. However newer stores like Azure Cosmos DB (previously knows as Azure Document DB) that wraps Table Store inside of its scope has better support. One should explore those options first..
