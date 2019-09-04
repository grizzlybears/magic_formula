# -*- coding: utf-8 -*-
import io
import os
import csv
import data_struct

def generate_his_csv( code1, code2, logged_his):
    filename = "%s/%s_%s.csv" % (data_struct.WORKING_DIR, code1, code2)
    #the_file = io.open( filename, "w", encoding='utf-8')
    the_file = io.open( filename, "wb" )

    writer = csv.writer(the_file)
    #writer.writerow ( fieldnames)
    writer.writerows ( logged_his)

    the_file.close()
 
def generate_csv( basename, header, data):
    filename = "%s/%s.csv" % (data_struct.WORKING_DIR, basename)
    #the_file = io.open( filename, "w", encoding='utf-8')
    the_file = io.open( filename, "wb" )

    writer = csv.writer(the_file)
    writer.writerow ( header )
    writer.writerows ( data)

    the_file.close()
    
def array_content_to_file(the_file, var_name, the_array): 
    the_file.write( "var %s = [\n" % var_name  );
    for i, row in enumerate( the_array):
        if 0 == i:
            the_file.write( "\t%s\n"  %  str(row).decode('string_escape'))
        else:
            the_file.write( "\t,%s\n" %  str(row).decode('string_escape'))

    #the_file.write( str( the_array).decode('string_escape'))
    the_file.write( "];\n"   );



def generate_js_data_w_head( jsfilename,  logged_his):
    with io.open( jsfilename, "wb" ) as js_file:
        js_file.write( "var None = null;\n" );   # Python 用 'None' , js 要求 'null'
        array_content_to_file( js_file, "header" , logged_his[0:1])
        array_content_to_file( js_file, "raw_data" , logged_his[1:])

def generate_js_head_n_data( jsfilename, header, data ):
    with io.open( jsfilename, "wb" ) as js_file:
        js_file.write( "var None = null;\n" );   # Python 用 'None' , js 要求 'null'
        array_content_to_file( js_file, "header" , [header])
        array_content_to_file( js_file, "raw_data" , data)

def write_chart_html_header( the_file, jsfilename, html_title):
    the_file.write(" <html>\n<head>\n<title> %s </title>\n" % html_title)

    the_file.write(" <script type=\"text/javascript\" src=\"ggchart_loader.js\"></script>\n")
    the_file.write(" <script type=\"text/javascript\" src=\"%s\"></script>\n" %  jsfilename )

    the_file.write( 
    '''
    <script type="text/javascript">
      google.charts.load('current', {'packages':['corechart']});
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {
      ''');
    

def draw_chart_full( the_file, chart_title, additional_options = ""):

    s = '''
        var options = {
          title: '$title$',
          curveType: 'function',
          legend: { position: 'bottom' }
          , height:550
          %s
        };

        var chart1 = new google.visualization.LineChart(document.getElementById('curve_chart1'));
        var data1 = google.visualization.arrayToDataTable(
                header.concat( raw_data)
                );
        chart1.draw(data1, options);


        ''' % additional_options 

    the_file.write( 
            s.replace( 
                '$title$' ,  chart_title  
                )
            )

def draw_chart_w_anno_full( the_file, chart_title, header , additional_options = ""):

    s = '''
        var options = {
          title: '%s',
          curveType: 'function',
          legend: { position: 'bottom' }
          , height:550
          %s
        };

        ''' % (chart_title, additional_options )
    the_file.write(s);


    s =  '''
        var chart1 = new google.visualization.LineChart(document.getElementById('curve_chart1'));
        var data1  = new google.visualization.DataTable();
        '''  
    the_file.write(s);

    for i, col in enumerate( header ):
        if 0 == i :
            the_file.write(
                    "data1.addColumn('string', '%s');\n" % col
            )
        else:
            the_file.write(
                    "data1.addColumn('number', '%s');\n" % col
            )
            

    s = '''
        data1.addColumn({type:'string', role:'annotation'}); // annotation role col.
        data1.addColumn({type:'string', role:'annotationText'}); // annotationText col.
     
        '''
    the_file.write(s);

    s = '''
        data1.addRows( raw_data );
        chart1.draw(data1, options);
    
        '''
    the_file.write(s);

  
def chart_div_full(the_file):    
    the_file.write(" <div id=\"curve_chart1\" ></div>\n")

def draw_chart_w_anno_last_x( the_file, chart_title, header ,x, subvar , additional_options = ""):

    s = '''
        var options_%d = {
          title: '%s, last %d',
          curveType: 'function',
          legend: { position: 'bottom' }
          , height:550
          %s
        };

        ''' % (subvar, chart_title, x, additional_options )
    the_file.write(s);


    s =  '''
        var chart_%d = new google.visualization.LineChart(document.getElementById('curve_chart_%d'));
        var data_%d  = new google.visualization.DataTable();
        '''  % (subvar, subvar , subvar ) 
    the_file.write(s);

    for i, col in enumerate( header ):
        if 0 == i :
            the_file.write(
                    "\tdata_%d.addColumn('string', '%s');\n" % (subvar, col )
            )
        else:
            the_file.write(
                    "\tdata_%d.addColumn('number', '%s');\n" % (subvar,  col)
            )
            

    s = '''
        data_%d.addColumn({type:'string', role:'annotation'}); // annotation role col.
        data_%d.addColumn({type:'string', role:'annotationText'}); // annotationText col.
     
        ''' % (subvar , subvar) 
    the_file.write(s);

    s = '''
        data_%d.addRows( raw_data.slice( - %d  ));
        chart_%d.draw(data_%d, options_%d);
    
        ''' % (subvar, x, subvar , subvar, subvar ) 
    the_file.write(s);


def draw_chart_last_x( the_file, chart_title, x , subvar , additional_options = "" ): 

    the_file.write( 
            '''var options_%d = { 
                title: '%s, last %d', 
                curveType: 'function', 
                legend: { position: 'bottom' } , height:550
                %s  
                };\n
                '''
                % (subvar, chart_title, x , additional_options )
                )

    the_file.write("    var chart_%d = new google.visualization.LineChart(document.getElementById('curve_chart_%d'));\n" 
            %  (subvar, subvar)
            )
 
    the_file.write("    var data_%d = google.visualization.arrayToDataTable( header.concat( raw_data.slice( - %d  ) ));\n" 
            % (subvar, x ) 
            )

    the_file.write("    chart_%d.draw(data_%d, options_%d);\n\n" % (subvar, subvar,  subvar) 
            )

def chart_div_subvar(the_file, subvar):    
    the_file.write(" <div id=\"curve_chart_%d\"></div>\n" % subvar)

def head_end_body_begin( the_file):
    the_file.write("}\n </script>\n </head>\n <body>\n")


def html_end( the_file):
    the_file.write(" </body>\n</html>\n")


# 'header' : 1D数组，列名 ，第一列是横坐标
def make_base_name(header):
    basename = ""
    for i, col in  enumerate(header):
        if 0 == i:
            continue
        elif 1 == i:
            basename = basename + str( col)
        else:
            basename = basename + "_" + str( col)

    return basename

# 'data' : 2-D array 
#       横坐标   line1  line2  line3 line3上的标注  line3上的标注详细   
#
def generate_htm_chart_for_faster_horse( sec1, sec2, data , suffix):
    basename = "%s_%s_faster_horse%s" % ( sec1.code, sec2.code, suffix ) 

    header = [
        '日期'
        , str(sec1) 
        , str(sec2)
        , "换快马"
        ]

    jsfilename = "%s.js" % ( basename, )
    jsfilepath = "%s/%s" % (data_struct.WORKING_DIR, jsfilename) 
    generate_js_head_n_data (jsfilepath,  header , data)
 
    filename = "%s/%s.html" % (data_struct.WORKING_DIR, basename)

    with io.open( filename, "wb" ) as the_file:
        write_chart_html_header( the_file, jsfilename,  "换快马 %s %s %s" % (sec1, sec2, suffix) ) 
        draw_chart_w_anno_full( the_file,  basename , header )
    
        if len(data) > 1000:
            draw_chart_w_anno_last_x( the_file, basename , header,  600 , 1 )
            draw_chart_w_anno_last_x( the_file, basename , header,  300 , 2 )

        head_end_body_begin( the_file)
        chart_div_full(the_file)
     
        if len(data) > 1000:
        
            chart_div_subvar( the_file,1 )
            chart_div_subvar( the_file,2 )


    return 

# 'data' : 2-D array 
#
#     横坐标   line1  line2  ... lineN lineN上的标注  lineN上的标注详细   
#                                只有最后一根line N 是有标注的
#
def generate_htm_chart_for_faster_horse2( secs,  data , suffix):
    basename=""
    header = ['日期'] 
    title_nut  = ""
    
    for i, sec in enumerate(secs):
        if 0==i:
            basename = sec.code
            title_nut = str(sec)
        else:
            basename = basename + "_" + sec.code
            title_nut = " " + str(sec)
        
        header.append( str(sec))

    basename = basename + "_fh2%s" % suffix 
    header.append('换快马')

    jsfilename = "%s.js" % ( basename, )
    jsfilepath = "%s/%s" % (data_struct.WORKING_DIR, jsfilename) 
    generate_js_head_n_data (jsfilepath,  header , data)
 
    filename = "%s/%s.html" % (data_struct.WORKING_DIR, basename)

    with io.open( filename, "wb" ) as the_file:
        write_chart_html_header( the_file, jsfilename,  "换快马 %s %s" % (title_nut, suffix) ) 
        draw_chart_w_anno_full( the_file,  basename , header )
    
        if len(data) > 1000:
            draw_chart_w_anno_last_x( the_file, basename , header,  600 , 1 )
            draw_chart_w_anno_last_x( the_file, basename , header,  300 , 2 )

        head_end_body_begin( the_file)
        chart_div_full(the_file)
     
        if len(data) > 1000:
        
            chart_div_subvar( the_file,1 )
            chart_div_subvar( the_file,2 )
    return 
          

# 'header' : 1D数组，列名 ，第一列是横坐标
# 'data' :   2D数组，数据
def simple_generate_line_chart( header, data):
    basename = make_base_name(header)
    jsfilename = "%s.js" % ( basename, )
    jsfilepath = "%s/%s" % (data_struct.WORKING_DIR, jsfilename) 
    generate_js_head_n_data (jsfilepath,  header , data)
 
    filename = "%s/%s.html" % (data_struct.WORKING_DIR, basename)
    with io.open( filename, "wb" ) as the_file:
        write_chart_html_header( the_file, jsfilename,   basename ) 
        draw_chart_full( the_file,  basename )

        if len(data) > 1000:
            draw_chart_last_x( the_file, basename , 600 , 1 )
            draw_chart_last_x( the_file, basename , 300 , 2 )

        head_end_body_begin( the_file)
        chart_div_full(the_file)
        
        if len(data) > 1000:
        
            chart_div_subvar( the_file,1 )
            chart_div_subvar( the_file,2 )

        html_end( the_file)



def generate_his_htm_chart( sec1, sec2, logged_his):
    
    jsfilename = "%s_%s.js" % ( sec1.code, sec2.code)
    jsfilepath = "%s/%s" % (data_struct.WORKING_DIR, jsfilename) 
    generate_js_data_w_head(jsfilepath,  logged_his )
 
    filename = "%s/%s_%s.html" % (data_struct.WORKING_DIR, sec1.code, sec2.code)
    with io.open( filename, "wb" ) as the_file:
        write_chart_html_header( the_file, jsfilename,  "历史对比 %s %s" % (sec1, sec2) ) 
        draw_chart_full( the_file, "%s %s, 全" % (sec1, sec2) )

        if len(logged_his) > 1000:
            chart_title = "%s %s" % (sec1, sec2)
            draw_chart_last_x( the_file, chart_title , 600 , 1 )
            draw_chart_last_x( the_file, chart_title , 300 , 2 )

        head_end_body_begin( the_file)
        chart_div_full(the_file)
        
        if len(logged_his) > 1000:
        
            chart_div_subvar( the_file,1 )
            chart_div_subvar( the_file,2 )

        html_end( the_file)



def generate_diff_htm_chart( sec1, sec2, diff_his): 
    
    jsfilename = "%s_%s.diff.js" % ( sec1.code, sec2.code)
    jsfilepath = "%s/%s" % (data_struct.WORKING_DIR, jsfilename) 
    generate_js_data_w_head(jsfilepath,   diff_his )
    
    filename = "%s/%s_%s.diff.html" % (data_struct.WORKING_DIR, sec1.code, sec2.code)

    additional_options =  ',colors: [\'black\']' 
    with io.open( filename, "wb" ) as the_file:
        write_chart_html_header( the_file, jsfilename, " %s - %s 历史" % (sec1, sec2) ) 
        draw_chart_full( the_file, "%s - %s, 全" % (sec1, sec2) , additional_options  )

        if len(diff_his) > 1000: 
            chart_title = "%s - %s" % (sec1, sec2)
            draw_chart_last_x( the_file, chart_title , 600 , 1 , additional_options)
            draw_chart_last_x( the_file, chart_title , 300 , 2 , additional_options)

        head_end_body_begin( the_file)
        chart_div_full(the_file)
        
        if len(diff_his) > 1000:
        
            chart_div_subvar( the_file,1 )
            chart_div_subvar( the_file,2 )

        html_end( the_file)




