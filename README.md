Portia
======

Portia is a tool that allows you to visually scrape websites without any programming knowledge required. With Portia you can annotate a web page to identify the data you wish to extract, and Portia will understand based on these annotations how to scrape data from similar pages.

# Running Portia with vagrant

To run Portia is using Vagrant.

    git clone https://github.com/mraouf1/portia
    cd portia
    vagrant up
    vagrant ssh
    sudo service nginx restart
    sudo service slyd stop
    cd /vagrant/slyd
    sudo python bin/slyd -p 9002 --root ./dist
    
To start development on the ember app
    
    sudo apt-get install npm
    sudo apt-get install bower
    cd portia/slyd
    npm install -g ember-cli
    npm install && bower install
    
To start watching files and building ember build on development
    
    cd portia/slyd
    gulp
    
Or build ember manually using 

    ember build
    
# Portia with kipp 
    
For using portia with kipp first provision kipp as in https://github.com/flyingelephantlab/leon, then

    vagrant ssh
    cd /apps/
    git clone https://github.com/mraouf1/portia
    cd portia
    docker build -t portia ..
    sudo docker run -i -t --rm -v /apps/portia:/app -v /apps/kipp/kipp/kipp_base/kipp_settings/:/apps/kipp/kipp/kipp_base/kipp_settings -v /var/kipp/scrapely_templates:/var/kipp/scrapely_templates -p 9001:9001 --name portia portia
    
For more detailed instructions, and alternatives to using Vagrant, see the [Installation](http://portia.readthedocs.org/en/latest/installation.html) docs.

# Documentation

Documentation can be found [here](http://portia.readthedocs.org/en/latest/index.html). Source files can be found in the ``docs`` directory.
