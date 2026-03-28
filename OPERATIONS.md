# Operations

## Production

- Host: `root@5.129.199.228`
- Domain: `https://sales.id-smart.ru`
- App root: `/opt/sales-marketplaces`
- Backend service: `sales-marketplaces-backend`
- Nginx site: `/etc/nginx/sites-available/sales.id-smart.ru.conf`

## Deploy

```bash
ssh root@5.129.199.228
cd /opt/sales-marketplaces
./deploy.sh
```

## Health Checks

```bash
curl -s http://127.0.0.1:18000/api/health
curl -s http://127.0.0.1/api/health
curl -s https://sales.id-smart.ru/api/health
```

## Services

```bash
systemctl status sales-marketplaces-backend --no-pager -l
systemctl restart sales-marketplaces-backend
systemctl status nginx --no-pager -l
systemctl reload nginx
systemctl status postgresql --no-pager -l
```

## Postgres

- Databases:
  - `data_analytics_system`
  - `data_analytics_hot`
  - `data_analytics_history`
- Role: `sales_app`

Quick checks:

```bash
sudo -u postgres psql -lqt | grep data_analytics
sudo -u postgres psql -d data_analytics_system -c "select count(*) from stores;"
sudo -u postgres psql -d data_analytics_hot -c "select count(*) from pricing_price_results;"
sudo -u postgres psql -d data_analytics_hot -c "select count(*) from sales_overview_order_rows;"
```

## Backups

- Script: `/opt/sales-marketplaces/deploy/postgres_backup.sh`
- Backup dir: `/opt/backups/sales-marketplaces`
- Timer: `sales-marketplaces-backup.timer`

Manual run:

```bash
systemctl start sales-marketplaces-backup.service
journalctl -u sales-marketplaces-backup.service -n 100 --no-pager
ls -lh /opt/backups/sales-marketplaces
```

## Swap

Check:

```bash
free -h
swapon --show
```
