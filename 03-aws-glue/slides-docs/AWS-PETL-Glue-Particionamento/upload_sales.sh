
BUCKET="amzn-s3-glue-full-particionament"
BASE="s3://$BUCKET/sales/year=2025"

# --- JULIO (22 al 31) ---
for i in {1..10}; do
  local_file="sales_july${i}.csv"
  day=$((21 + i))  # 22..31
  if [[ -f "$local_file" ]]; then
    aws s3 cp "$local_file" "$BASE/month=7/day=${day}/"
    echo "Subido $local_file a $BASE/month=7/day=${day}/"
  else
    echo "Archivo $local_file no encontrado, se omite"
  fi
done

# --- AGOSTO (1 al 10) ---
for i in {1..10}; do
  local_file="sales_august${i}.csv"
  day=$i
  if [[ -f "$local_file" ]]; then
    aws s3 cp "$local_file" "$BASE/month=8/day=${day}/"
    echo "Subido $local_file a $BASE/month=8/day=${day}/"
  else
    echo "Archivo $local_file no encontrado, se omite"
  fi
done

echo "Carga finalizada en $BUCKET"
