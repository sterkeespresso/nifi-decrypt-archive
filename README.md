<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
[<img src="https://nifi.apache.org/assets/images/apache-nifi-logo.svg" width="300" height="126" alt="Apache NiFi"/>][nifi]

[![ci-workflow](https://github.com/apache/nifi/workflows/ci-workflow/badge.svg)](https://github.com/apache/nifi/actions)
[![Docker pulls](https://img.shields.io/docker/pulls/apache/nifi.svg)](https://hub.docker.com/r/apache/nifi/)
[![Version](https://img.shields.io/maven-central/v/org.apache.nifi/nifi-utils.svg)](https://nifi.apache.org/download.html)
[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://s.apache.org/nifi-community-slack)

[Apache NiFi](https://nifi.apache.org/) is an easy to use, powerful, and
reliable system to process and distribute data.

## Table of Contents

- [NiFi Decrypt Archive](#nifi-decrypt-archive)
- [License](#license)
- [Export Control](#export-control)

## NiFi Decrypt Archive

Decrypts and optionally unpacks the content of FlowFiles that have been
packaged and encrypted using the zip format.

This custom NiFi processor uses the Lingala Zip4j libraries to decrypt and
copy the contents of a zip file into a new unencrypted zip file or into one
or more unpacked files using streams.

## License

Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Export Control

This distribution includes cryptographic software. The country in which you
currently reside may have restrictions on the import, possession, use, and/or
re-export to another country, of encryption software. BEFORE using any
encryption software, please check your country's laws, regulations and
policies concerning the import, possession, or use, and re-export of encryption
software, to see if this is permitted. See <http://www.wassenaar.org/> for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and Security
(BIS), has classified this software as Export Commodity Control Number (ECCN)
5D002.C.1, which includes information security software using or performing
cryptographic functions with asymmetric algorithms. The form and manner of this
Apache Software Foundation distribution makes it eligible for export under the
License Exception ENC Technology Software Unrestricted (TSU) exception (see the
BIS Export Administration Regulations, Section 740.13) for both object code and
source code.

The following provides more details on the included cryptographic software:

Apache NiFi uses BouncyCastle, JCraft Inc., and the built-in
Java cryptography libraries for SSL, SSH, and the protection
of sensitive configuration parameters. See
http://bouncycastle.org/about.html
http://www.jcraft.com/c-info.html
http://www.oracle.com/us/products/export/export-regulations-345813.html
for more details on each of these libraries cryptography features.

[nifi]: https://nifi.apache.org/
[logo]: https://nifi.apache.org/assets/images/apache-nifi-logo.svg
