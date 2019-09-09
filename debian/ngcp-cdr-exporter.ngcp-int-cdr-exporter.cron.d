*/5 * * * *   root    . /etc/default/ngcp-roles; if /usr/sbin/ngcp-check-active -q && [ "$NGCP_IS_MGMT" = "yes" ] ; then /usr/sbin/ngcp-int-cdr-exporter >/dev/null; fi
