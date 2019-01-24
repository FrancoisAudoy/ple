var mymap = L.map('mapid').setView([51.505, -0.09], 13);
L.tileLayer('http://young:3000/tiles/{x}/{y}/{z}', {
    maxZoom: 1,
}).addTo(mymap);
