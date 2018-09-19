#!/usr/bin/env bash
curl -X PUT "http://localhost:9090/data/Role/4eea64e6-5c87-462d-9fc5-c3fdd3a310fa" -H "content-type: application/json; charset=utf-8" --data-binary "@src/test/resources/spec/v1/examples/RoleExample.json"
echo
echo "ADDED RESOURCE http://localhost:9090/data/Role/4eea64e6-5c87-462d-9fc5-c3fdd3a310fa"
curl "http://localhost:9090/data/Role/4eea64e6-5c87-462d-9fc5-c3fdd3a310fa"
curl -X PUT "http://localhost:9090/data/Agent/1d6ca477-4d0c-47b1-967d-1bead9c1b56b" -H "content-type: application/json; charset=utf-8" --data-binary "@src/test/resources/spec/v1/examples/AgentExample1.json"
echo
echo "ADDED RESOURCE http://localhost:9090/data/Agent/1d6ca477-4d0c-47b1-967d-1bead9c1b56b"
curl "http://localhost:9090/data/Agent/1d6ca477-4d0c-47b1-967d-1bead9c1b56b"
curl -X PUT "http://localhost:9090/data/Agent/91712998-84a5-4f6c-8053-4dfaafa7c0e3" -H "content-type: application/json; charset=utf-8" --data-binary "@src/test/resources/spec/v1/examples/AgentExample2.json"
echo
echo "ADDED RESOURCE http://localhost:9090/data/Agent/91712998-84a5-4f6c-8053-4dfaafa7c0e3"
curl "http://localhost:9090/data/Agent/91712998-84a5-4f6c-8053-4dfaafa7c0e3"
curl -X PUT "http://localhost:9090/data/AgentInRole/d1f8e3b7-0eb3-4a6d-91bb-a7451649f2f6" -H "content-type: application/json; charset=utf-8" --data-binary "@src/test/resources/spec/v1/examples/AgentInRoleExample.json"
echo
echo "ADDED RESOURCE http://localhost:9090/data/AgentInRole/d1f8e3b7-0eb3-4a6d-91bb-a7451649f2f6"
curl "http://localhost:9090/data/AgentInRole/d1f8e3b7-0eb3-4a6d-91bb-a7451649f2f6"
