# DBblue PostgreSQL 19 client tools on PATH
case ":$PATH:" in
  *:/opt/dbblue/19/bin:*) ;;
  *) export PATH="/opt/dbblue/19/bin:$PATH" ;;
esac
