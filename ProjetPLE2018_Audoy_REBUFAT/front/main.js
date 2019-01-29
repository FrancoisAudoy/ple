var mymap = L.map('mapid').setView([0.00, 0.00], 6);
L.tileLayer('http://young:8182/blackple/tiles/{z}/{x}/{y}', {
    maxZoom: 6,
    tileSize: 1201
}).addTo(mymap);
