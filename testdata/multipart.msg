From: "alice a" <alice@example.com>
Subject: hello
Message-id: <abc@xyz>
Date: Wed, 06 Mar 2024 10:42:31 -0800
content-type: multipart/mixed; boundary=xxx

--xxx
content-type: multipart/related; boundary=yyy

--yyy
content-type: multipart/alternative; boundary=zzz

--zzz
content-type: text/plain
hello
--zzz
content-type: text/html
<b>hello</b>
--zzz--
--yyy
content-type: image/png
content-id: xyz
content-transfer-encoding: base64

eW9sb2NhdA==
--yyy---
--xxx
content-type: image/png
content-disposition: attachment; filename="funny cats.png"
content-transfer-encoding: base64

eW9sb2NhdDI=
--xxx--
