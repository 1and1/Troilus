/*
 * Copyright (c) 2015 1&1 Internet AG, Germany, http://www.1und1.de
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.unitedinternet.troilus.example.utils.reactive.sse;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;


import com.google.common.base.Charsets;
import com.google.common.io.Closeables;


/**
 * SSEOutputStream
 * 
 * @author grro
 */
public class SSEOutputStream implements Closeable {
    

    private final OutputStream os;

    
    public SSEOutputStream(OutputStream os) {
        this.os = os;
    }
   

    @Override
    public synchronized void close() throws IOException {
        Closeables.close(os,  true);
    }
   
    
    public synchronized void write(SSEEvent sseEvent) throws IOException {
        os.write(sseEvent.toWire().getBytes(Charsets.UTF_8));
        flush();
    }
    
    
    public synchronized void flush() throws IOException {
        os.flush();
    }
}