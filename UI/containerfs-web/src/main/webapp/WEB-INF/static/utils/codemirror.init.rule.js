/**
 * Theme: Ubold Admin Template
 * Author: Coderthemes
 * Code Editors page
 */

!function ($) {
    "use strict";

    var CodeEditor = function () {
    };

    CodeEditor.prototype.init = function () {
        var $this = this;
        //init plugin
        var editor = CodeMirror.fromTextArea(document.getElementById("code"), {
            mode: {name: "xml", alignCDATA: true},
            lineNumbers: true,
            theme: 'ambiance'
        });

        //保存的时候赋值
        $('#saveform').click(function () {
            $('#ruleGroovy').val(editor.getValue());
        });
    },

    //init
    $.CodeEditor = new CodeEditor, $.CodeEditor.Constructor = CodeEditor
}(window.jQuery),

//initializing 
function ($) {
    "use strict";
    $.CodeEditor.init()
}(window.jQuery);
