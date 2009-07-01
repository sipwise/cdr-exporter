GRANT SELECT ON `accounting`.`cdr` TO 'exporter'@'192.168.102.%' identified by '1exportTheCDRs!';
GRANT SELECT, INSERT, UPDATE, DELETE ON `accounting`.`mark` TO 'exporter'@'192.168.102.%' identified by '1exportTheCDRs!';
