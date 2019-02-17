var Utils = {
    replaceQueryParam: function(key, newKeyValue) {
        var currentUrl = window.location.href;
        var queryChar = (currentUrl.indexOf("?") < 0) ? '?' : '&';

        if (currentUrl.indexOf(key) < 0) { // No key in query string
            return currentUrl + queryChar + newKeyValue;
        }

        // Key exists in query string
        // Matching by beginning of the key
        // to the end of query string value
        var regExp = new RegExp(key + "[a-zA-Z0-9]+([&]*)");
        return currentUrl.replace(regExp, newKeyValue + "&");
    },

    tableDataUrl: function(tableName) {
        return window.location.pathname + '?' + 'table=' + tableName;
    }
};

var TableDataView = {
   initTableChangeListener: function () {
       var tableSelector = document.getElementById("select-table");
       if (tableSelector == null) return;

       tableSelector.onchange = function () {
           var option = (typeof this.selectedIndex === "undefined" ? window.event.srcElement : this);
           var selectedTable = option.value || option.options[option.selectedIndex].value;
           window.location.href = Utils.tableDataUrl(selectedTable);
       };
   },

    initFormatChangeListener: function(){
        var formatSelector = document.getElementById("select-format");
        if (formatSelector == null) return;

        formatSelector.onchange = function () {
            var option = (typeof this.selectedIndex === "undefined" ? window.event.srcElement : this);
            var selectedFormat = option.value || option.options[option.selectedIndex].value;
            window.location.href = Utils.replaceQueryParam("format=", "format=" + selectedFormat)
        };
    },

    initLimitChangeListender: function () {
        var limitSelector = document.getElementById("select-limit");
        if (limitSelector == null) return;

        limitSelector.onchange = function () {
            var option = (typeof this.selectedIndex === "undefined" ? window.event.srcElement : this);
            var selectedLimit = option.value || option.options[option.selectedIndex].value;
            window.location.href = Utils.replaceQueryParam("limit=", "limit=" + selectedLimit)
        };
    }
};

(function () {
    TableDataView.initTableChangeListener();
    TableDataView.initFormatChangeListener();
    TableDataView.initLimitChangeListender();
})();