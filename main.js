var auto = require('autocomplete-element');
var globiData = require('globi-data');
var queryString = require('query-string');
var L = require('leaflet');

function createEllipsis() {
    var ellipsis = document.createElement('td');
    ellipsis.textContent = '...';
    return ellipsis;
}
function renderGBIF(occurrences, resp) {
    occurrences.setAttribute('data-results', resp.results);
    var headerRow = document.createElement('tr');
    var header = document.createElement('th');
    header.textContent = 'taxon occurrences';
    headerRow.appendChild(header);
    header = document.createElement('th');
    header.textContent = '(lat,lng)';
    headerRow.appendChild(header);
    occurrences.appendChild(headerRow);
    resp.results.forEach(function (occurrence) {
        var row = document.createElement('tr');
        var path = document.createElement('td');
        var pathElems = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species'].reduce(function (pathFull, pathPart) {
            var pathPartValue = occurrence[pathPart];
            if (pathPartValue !== undefined) {
                var pathPartElem = document.createElement('a');
                pathPartElem.setAttribute('href', 'http://eol.org/' + pathPartValue);
                pathPartElem.textContent = pathPartValue;
                pathPartElem.setAttribute('title', 'search EOL for [' + pathPartValue + '] by name');
                var sepElem = document.createElement('span');
                sepElem.textContent = ' | ';
                return pathFull.concat([pathPartElem, sepElem])
            } else {
                return pathFull;
            }
        }, []);
        pathElems.forEach(function (elem) {
            path.appendChild(elem);
        });
        row.appendChild(path);
        var latLng = document.createElement('td');
        latLng.textContent = '(' + occurrence.decimalLatitude + ',' + occurrence.decimalLongitude + ')';
        row.appendChild(latLng);
        occurrences.appendChild(row);
    });
    var ellipsisRow = document.createElement('tr');
    ellipsisRow.appendChild(createEllipsis());
    ellipsisRow.appendChild(createEllipsis());
    occurrences.appendChild(ellipsisRow);
}

function sepElem() {
    var sepElem = document.createElement('span');
    sepElem.textContent = ' | ';
    return sepElem;
}
function renderChecklist(checklist, resp) {
    checklist.setAttribute('data-results', resp.results);
    var headerRow = document.createElement('tr');
    var header = document.createElement('th');
    header.textContent = 'checklist items';
    headerRow.appendChild(header);
    header = document.createElement('th');
    header.textContent = 'occurrence count';
    headerRow.appendChild(header);
    checklist.appendChild(headerRow);
    resp.items.forEach(function (item) {
        var row = document.createElement('tr');
        var path = document.createElement('td');
        var pathElems = item.taxon.split('|').reduce(function (pathFull, pathPartValue) {
            if (pathPartValue.length == 0) {
                return pathFull.concat([sepElem()]);
            } else {
                var pathPartElem = document.createElement('a');
                pathPartElem.setAttribute('href', 'http://eol.org/' + pathPartValue);
                pathPartElem.textContent = pathPartValue;
                pathPartElem.setAttribute('title', 'search EOL for [' + pathPartValue + '] by name');
                return pathFull.concat([pathPartElem, sepElem()])
            }
        }, []);
        pathElems.forEach(function (elem) {
            path.appendChild(elem);
        });
        row.appendChild(path);
        var recordCount = document.createElement('td');
        recordCount.textContent = item.recordcount;
        row.appendChild(recordCount);
        checklist.appendChild(row);
    });
    var ellipsisRow = document.createElement('tr');
    ellipsisRow.appendChild(createEllipsis());
    ellipsisRow.appendChild(createEllipsis());
    checklist.appendChild(ellipsisRow);
}

function xhr() {
    var req = null;
    if (window.XMLHttpRequest) { // Mozilla, Safari, ...
        req = new XMLHttpRequest();
    } else if ((typeof window !== 'undefined') && window.ActiveXObject) { //     IE
        try {
            req = new ActiveXObject('Msxml2.XMLHTTP');
        } catch (e) {
            try {
                req = new ActiveXObject('Microsoft.XMLHTTP');
            } catch (e) {
            }
        }
    }
    return req;
}
var updateOccurrences = function () {
    var req = xhr();
    if (req !== undefined) {
        var baseUrl = 'http://api.gbif.org/v1/occurrence/search';
        var dataFilter = getDataFilter();
        var query = Object.keys(dataFilter).reduce(function (accum, key) {
            if (dataFilter[key] !== null) {
                return accum + key + '=' + encodeURIComponent(dataFilter[key]) + '&';
            } else {
                return accum;
            }
        }, '?');

        var url = baseUrl + query;
        req.open('GET', url, true);
        req.onreadystatechange = function () {
            if (req.readyState === 4) {
                setOccurrenceStatus('received response');
                if (req.status === 200) {
                    var resp = JSON.parse(req.responseText);
                    setOccurrenceStatus('got [' + resp.count + '] matches from gbif');
                    if (resp.results) {
                        renderGBIF(occurrences, resp);
                    }
                } else {
                    setOccurrenceStatus('not ok. Received a [' + req.status + '] status with text [' + req.statusText + '] in response to [' + url + ']');
                }
            }
        };
        removeChildren('#occurrences');
        setOccurrenceStatus('building new list...');
        req.send(null);
    }
};

var removeChildren = function (selector) {
    var checklist = document.querySelector(selector);
    while (checklist.firstChild) {
        checklist.removeChild(checklist.firstChild);
    }
    return checklist;
};


function clearChecklist() {
    removeChildren('#checklist');
    removeChildren('#download');
    setChecklistStatus('none requested');
}

var createChecklistURL = function (dataFilter) {
    return 'http://apihack-c18.idigbio.org:8888/checklist' + Object.keys(dataFilter).filter(function (key) {
        return ['taxonSelector', 'wktString', 'limit'].indexOf(key) != -1;
    }).reduce(function (accum, key) {
        if (dataFilter[key] !== null) {
            return accum + key + '=' + encodeURIComponent(dataFilter[key]) + '&';
        } else {
            return accum;
        }
    }, '?');
};

var updateDownloadURL = function () {
    removeChildren("#download");

    var dataFilter = getDataFilter();
    dataFilter.limit = 1024 * 4;

    var download = document.querySelector('#download');
    download.appendChild(document.createElement("span"))
        .textContent = 'download up to [' + dataFilter.limit + '] checklist items as ';

    var url = createChecklistURL(dataFilter);
    var jsonRef = download.appendChild(document.createElement("a"));
    jsonRef.setAttribute('href', url);
    jsonRef.textContent = 'json';

    var req = xhr();
    if (req !== undefined) {
        req.open('GET', url, true);
        req.onreadystatechange = function () {
            if (req.readyState === 4) {
                if (req.status === 200) {
                    var resp = JSON.parse(req.responseText);
                    if (resp.items) {
                        var csvString = resp.items.reduce(function (agg, item) {
                            if (item.taxon && item.recordcount) {
                                agg = agg.concat([item.taxon, item.recordcount].join(','));
                            }
                            return agg;
                        }, ['taxon path,record count']).join('\n');
                        download.appendChild(document.createElement("span")).textContent = ' or as ';
                        var csvRef = download.appendChild(document.createElement("a"));
                        csvRef.setAttribute('href', encodeURI('data:text/csv;charset=utf-8,' + csvString));
                        csvRef.setAttribute('download', 'checklist.csv')
                        csvRef.textContent = 'csv';
                    }
                }
            }
        };
        req.send(null);
    }
}


var updateChecklist = function () {
    var req = xhr();
    if (req !== undefined) {
        req.open('GET', createChecklistURL(getDataFilter()), true);
        req.onreadystatechange = function () {
            if (req.readyState === 4) {
                setChecklistStatus('received response');
                if (req.status === 200) {
                    var resp = JSON.parse(req.responseText);
                    setChecklistStatus(resp.status);
                    if (resp.items) {
                        renderChecklist(checklist, resp);
                        if (resp.items.length > 0) {
                            updateDownloadURL();
                        }
                    }
                } else {
                    setChecklistStatus('not ok. Received a [' + req.status + '] status with text [' + req.statusText + '] in response to [' + createChecklistURL(getDataFilter()) + ']');
                }
            }
        };
        clearChecklist();
        setChecklistStatus('requesting checklist...');
        req.send(null);
    }
};

var setOccurrenceStatus = function (status) {
    document.querySelector('#occurrenceStatus').textContent = status;
};

var setChecklistStatus = function (status) {
    document.querySelector('#checklistStatus').textContent = status;
};

var getDataFilter = function () {
    var occurrences = document.querySelector('#checklist');
    var dataFilter = { hasSpatialIssue: 'false', limit: 20 };
    if (occurrences.hasAttribute('data-filter')) {
        dataFilter = JSON.parse(occurrences.getAttribute('data-filter'));
    }
    return dataFilter;
}

var setDataFilter = function (dataFilter) {
    var dataFilterString = JSON.stringify(dataFilter);
    document.querySelector('#checklist').setAttribute('data-filter', dataFilterString);
    document.location.hash = queryString.stringify(dataFilter);
}

function updateTaxonSelector() {
    var filterElems = Array.prototype.slice.call(document.querySelectorAll('.taxonFilterElementName'));

    var filterJoin = filterElems.reduce(function (filterAgg, filterElem) {
        var taxonName = filterElem.textContent.trim();
        if (taxonName.length > 0) {
            filterAgg = filterAgg.concat(filterElem.textContent.trim());
        }
        return filterAgg;
    }, []).join(',');

    var filter = getDataFilter();
    filter.scientificName = filterJoin;
    filter.taxonSelector = filterJoin;
    setDataFilter(filter);
}

var updateBBox = function (areaSelect) {
    var bounds = areaSelect.getBounds();
    var wktPoints = bounds._northEast.lng + ' ' + bounds._northEast.lat
        + ',' + bounds._northEast.lng + ' ' + bounds._southWest.lat
        + ',' + bounds._southWest.lng + ' ' + bounds._southWest.lat
        + ',' + bounds._southWest.lng + ' ' + bounds._northEast.lat
        + ',' + bounds._northEast.lng + ' ' + bounds._northEast.lat;

    var dataFilter = getDataFilter();
    dataFilter.geometry = 'POLYGON((' + wktPoints + '))';

    var lngMin = Math.min(bounds._northEast.lng, bounds._southWest.lng);
    var lngMax = Math.max(bounds._northEast.lng, bounds._southWest.lng);
    var latMin = Math.min(bounds._northEast.lat, bounds._southWest.lat);
    var latMax = Math.max(bounds._northEast.lat, bounds._southWest.lat);
    dataFilter.wktString = 'ENVELOPE(' + [lngMin, lngMax, latMax, latMin].join(',') + ')';

    dataFilter.zoom = areaSelect.map.getZoom();
    dataFilter.lat = areaSelect.map.getCenter().lat;
    dataFilter.lng = areaSelect.map.getCenter().lng;
    dataFilter.width = areaSelect._width;
    dataFilter.height = areaSelect._height;

    setDataFilter(dataFilter);
}

var init = function () {
    var addRequestHandler = function (buttonId) {
        var checklistButton = document.querySelector(buttonId);
        checklistButton.addEventListener('click', function (event) {
            updateChecklist();
        }, false);
    }

    clearChecklist();
    ['#requestChecklist', '#refreshChecklist'].forEach(function (id) {
        addRequestHandler(id)
    });

    var updateLists = function () {
        clearChecklist();
        updateOccurrences();
    };

    var addTaxonFilterElement = function (taxonName) {
        var taxonDiv = document.createElement('span');
        taxonDiv.setAttribute('class', 'taxonFilterElement');
        var removeButton = document.createElement('button');

        removeButton.addEventListener('click', function (event) {
            taxonDiv.parentNode.removeChild(taxonDiv);
            updateTaxonSelector();
            updateLists();
        });
        removeButton.textContent = 'x';

        var taxonNameSpan = document.createElement('span');
        taxonNameSpan.setAttribute('class', 'taxonFilterElementName');
        taxonNameSpan.textContent = taxonName;
        taxonDiv.appendChild(removeButton);
        taxonDiv.appendChild(taxonNameSpan);
        document.querySelector('#taxonFilter').appendChild(taxonDiv);
        updateTaxonSelector();
        updateLists();
    }

    var dataFilter = queryString.parse(document.location.hash);

    var zoom = parseInt(dataFilter.zoom || 7);
    var lat = parseFloat(dataFilter.lat || 42.31);
    var lng = parseFloat(dataFilter.lng || -71.05);

    var map = L.map('map').setView([lat, lng], zoom);

    L.tileLayer('https://{s}.tiles.mapbox.com/v3/{id}/{z}/{x}/{y}.png', {
        maxZoom: 18,
        attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, ' +
            '<a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, ' +
            'Imagery © <a href="http://mapbox.com">Mapbox</a>',
        id: 'examples.map-i875mjb7'
    }).addTo(map);

    var width = parseInt((dataFilter.width || 200));
    var height = parseInt((dataFilter.height || 200));
    var areaSelect = L.areaSelect({width: width, height: height});
    areaSelect.addTo(map);
    areaSelect.on("change", function () {
        updateBBox(this);
        updateLists();
    });

    var taxonFilterNames = (dataFilter.scientificName && dataFilter.scientificName.split(',')) || ['Aves', 'Insecta'];

    taxonFilterNames.forEach(function (taxonName) {
        addTaxonFilterElement(taxonName);
    });

    var nameInput = document.getElementById('scientificName');

    nameInput.onkeyup = function (event) {
        var suggestions = removeChildren('#suggestions');

        if (nameInput.value.length > 0) {
            var closeMatchCallback = function (closeMatches) {
                var instructions = document.createElement('div');
                instructions.textContent = 'click any button below to add taxon to filter';
                suggestions.appendChild(instructions);
                function addTaxonButton(taxonLabel, scientificName, suggestion) {
                    var addTaxonButton = document.createElement('button');
                    addTaxonButton.addEventListener('click', function (event) {
                        addTaxonFilterElement(scientificName.trim());
                    }, false);
                    addTaxonButton.textContent = taxonLabel;
                    suggestion.appendChild(addTaxonButton);
                }

                closeMatches.forEach(function (closeMatch) {
                    var suggestion = document.createElement('div');
                    suggestion.setAttribute('class', 'suggestion');
                    suggestion.setAttribute('data-suggestion', closeMatch);
                    if (closeMatch.commonNames.en) {
                        addTaxonButton(closeMatch.commonNames.en, closeMatch.scientificName, suggestion);
                    }
                    closeMatch.path.forEach(function (pathElem) {
                        addTaxonButton(pathElem, pathElem, suggestion);
                    });

                    suggestions.appendChild(suggestion);
                });
            };
            globiData.findCloseTaxonMatches(nameInput.value.trim(), closeMatchCallback);
        }
    };

    updateTaxonSelector();
    updateBBox(areaSelect);
    updateLists();
}

window.addEventListener('load', function () {
    init();
});
