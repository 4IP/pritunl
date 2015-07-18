pritunl: enterprise vpn server
==============================

.. image:: https://img.shields.io/badge/package-ubuntu-dd4814.svg?style=flat
    :target: https://launchpad.net/~pritunl/+archive/ubuntu/ppa

.. image:: https://img.shields.io/badge/package-arch%20linux-33aadd.svg?style=flat
    :target: https://aur.archlinux.org/packages/pritunl/

.. image:: https://img.shields.io/badge/package-centos-669900.svg?style=flat
    :target: https://pritunl.com/#install

.. image:: https://img.shields.io/badge/github-pritunl-11bdc2.svg?style=flat
    :target: https://github.com/pritunl

.. image:: https://img.shields.io/badge/twitter-pritunl-55acee.svg?style=flat
    :target: https://twitter.com/pritunl

`Pritunl <https://github.com/pritunl/pritunl>`_ is a distributed enterprise
vpn server built using the OpenVPN protocol. Documentation and more
information can be found at the home page `pritunl.com <https://pritunl.com>`_

.. image:: www/img/logo_code.png
    :target: https://pritunl.com

Development Setup (Single Node)
-------------------------------

.. code-block:: bash

    $ git clone https://github.com/pritunl/pritunl.git
    $ cd pritunl
    $ vagrant up mongodb node0
    $ vagrant ssh node0
    $ cd /vagrant
    $ sudo python2 server.py
    # Open http://localhost:9700/

Development Setup (Multi Node)
------------------------------

.. code-block:: bash

    $ git clone https://github.com/pritunl/pritunl.git
    $ cd pritunl
    $ vagrant up
    $ foreman start mongodb node0 node1 node2 node3 node4 node5
    # Open node0 http://localhost:9700/
    # Open node1 http://localhost:9701/
    # Open node2 http://localhost:9702/
    # Open node3 http://localhost:9703/
    # Open node4 http://localhost:9704/
    # Open node5 http://localhost:9705/

License
-------

Please refer to the `LICENSE` file for a copy of the license.

Export Requirements
-------------------

You may not export or re-export this software or any copy or adaptation in
violation of any applicable laws or regulations.

Without limiting the generality of the foregoing, hardware, software,
technology or services provided under this license agreement may not be
exported, reexported, transferred or downloaded to or within (or to a national
resident of) countries under U.S. economic embargo including the following
countries:

Cuba, Iran, Libya, North Korea, Sudan and Syria. This list is subject to
change.

Hardware, software, technology or services may not be exported, reexported,
transferred or downloaded to persons or entities listed on the U.S. Department
of Commerce Denied Persons List, Entity List of proliferation concern or on
any U.S. Treasury Department Designated Nationals exclusion list, or to
parties directly or indirectly involved in the development or production of
nuclear, chemical, biological weapons or in missile technology programs as
specified in the U.S. Export Administration Regulations (15 CFR 744).

By accepting this license agreement you confirm that you are not located in
(or a national resident of) any country under U.S. economic embargo, not
identified on any U.S. Department of Commerce Denied Persons List, Entity List
or Treasury Department Designated Nationals exclusion list, and not directly
or indirectly involved in the development or production of nuclear, chemical,
biological weapons or in missile technology programs as specified in the U.S.
Export Administration Regulations.

Software available on this web site contains cryptography and is therefore
subject to US government export control under the U.S. Export Administration
Regulations ("EAR"). EAR Part 740.13(e) allows the export and reexport of
publicly available encryption source code that is not subject to payment of
license fee or royalty payment. Object code resulting from the compiling of
such source code may also be exported and reexported under this provision if
publicly available and not subject to a fee or payment other than reasonable
and customary fees for reproduction and distribution. This kind of encryption
source code and the corresponding object code may be exported or reexported
without prior U.S. government export license authorization provided that the
U.S. government is notified about the Internet location of the software.

The software available on this web site is publicly available without license
fee or royalty payment, and all binary software is compiled from the source
code. The U.S. government has been notified about this site and the location
site for the source code. Therefore, the source code and compiled object code
may be downloaded and exported under U.S. export license exception (without a
U.S. export license) in accordance with the further restrictions outlined
above regarding embargoed countries, restricted persons and restricted end
uses.

Local Country Import Requirements. The software you are about to download
contains cryptography technology. Some countries regulate the import, use
and/or export of certain products with cryptography. Pritunl makes no
claims as to the applicability of local country import, use and/or export
regulations in relation to the download of this product. If you are located
outside the U.S. and Canada you are advised to consult your local country
regulations to insure compliance.
