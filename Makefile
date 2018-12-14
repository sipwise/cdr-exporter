INSTALL = install
INSTALL_PROGRAM = $(INSTALL)
INSTALL_DIR = $(INSTALL) -d
INSTALL_DATA = $(INSTALL) -m 644

all:

install:
	$(INSTALL_DIR)					$(DESTDIR)/usr/sbin
	$(INSTALL_PROGRAM) cdr-md5.sh 			$(DESTDIR)/usr/sbin/cdr-md5
	$(INSTALL_PROGRAM) cdr-exporter.pl 		$(DESTDIR)/usr/sbin/cdr-exporter
	$(INSTALL_PROGRAM) event-exporter.pl		$(DESTDIR)/usr/sbin/event-exporter
	$(INSTALL_DIR)					$(DESTDIR)/etc/ngcp-cdr-exporter
	$(INSTALL_DATA) cdr-exporter.conf		$(DESTDIR)/etc/ngcp-cdr-exporter/cdr-exporter.conf
	$(INSTALL_DATA) event-exporter.conf		$(DESTDIR)/etc/ngcp-cdr-exporter/event-exporter.conf
