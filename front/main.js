var mymap = L.map('mapid').setView([0.00, 0.00], 9);
L.tileLayer('http://young:8182/blackple/tiles/{z}/{x}/{y}', {
    maxZoom: 9,
    minZoom: 8,
    tileSize: 1201
}).addTo(mymap);
