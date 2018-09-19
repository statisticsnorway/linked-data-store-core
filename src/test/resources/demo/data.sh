#!/bin/bash

read
echo
echo
echo
echo "Create ProvisionAgreement Sirius"
echo
http -p HBhb -j PUT localhost:9090/data/provisionagreement/2a41c < 1-sirius.json

read
echo
echo
echo
echo "Get Sirius"
echo
http -p HBhb localhost:9090/data/provisionagreement/2a41c

read
echo
echo
echo
echo "Add Address to Sirius"
echo
http -p HBhb -j PUT localhost:9090/data/provisionagreement/2a41c/address < 2-sirius-address.json

read
echo
echo
echo
echo "Get Sirius"
echo
http -p HBhb localhost:9090/data/provisionagreement/2a41c

read
echo
echo
echo
echo "Create contact Skrue"
echo
http -p HBhb -j PUT localhost:9090/data/contact/4b2ef < 3-skrue.json

read
echo
echo
echo
echo "Get Skrue"
echo
http -p HBhb localhost:9090/data/contact/4b2ef

read
echo
echo
echo
echo "Create contact Donald"
echo
http -p HBhb -j PUT localhost:9090/data/contact/821aa < 4-donald.json

read
echo
echo
echo
echo "Get Donald"
echo
http -p HBhb localhost:9090/data/contact/821aa

read
echo
echo
echo
echo "Add Skrue to Sirius contacts"
echo
http -p HBhb -j PUT localhost:9090/data/provisionagreement/2a41c/contacts/contact/4b2ef

read
echo
echo
echo
echo "Get Sirius"
echo
http -p HBhb localhost:9090/data/provisionagreement/2a41c

read
echo
echo
echo
echo "Add Donald to Sirius contacts"
echo
http -p HBhb -j PUT localhost:9090/data/provisionagreement/2a41c/contacts/contact/821aa

read
echo
echo
echo
echo "Get Sirius"
echo
http -p HBhb localhost:9090/data/provisionagreement/2a41c

read
echo
echo
echo
echo "Delete Skrue from Sirius contacts"
echo
http -p HBhb DELETE localhost:9090/data/provisionagreement/2a41c/contacts/contact/4b2ef

read
echo
echo
echo
echo "Get Sirius"
echo
http -p HBhb localhost:9090/data/provisionagreement/2a41c
