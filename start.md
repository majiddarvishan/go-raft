# ۱. extract
tar -xzf authenticator.tar.gz && cd final

# ۲. بالا آوردن با Docker (همه چیز خودکار)
docker-compose up --build -d

# ۳. بررسی وضعیت
docker-compose logs -f node1

# ۴. اتصال با client
go run ./cmd/client --server=localhost:9001

# ─── نمونه دستورات ───────────────────────────────
[localhost:9001]> status              # وضعیت cluster
[localhost:9001]> list-configs        # ClientConfigها در FSM
[localhost:9001]> register smpp-client-1 10.0.0.5

# ترمینال ۱ — watch:
[localhost:9002]> watch my-session

# ترمینال ۲ — broadcast:
[localhost:9001]> broadcast disconnect "maintenance"
✓ broadcast to 1 client(s)