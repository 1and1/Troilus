/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus.example.service;

import java.util.concurrent.CompletableFuture;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;



public class Thumbnails {
    
    
    public static Thumbnail of(byte [] picture) {
        return new Thumbnail(picture);
    }
    
    
    public static class Thumbnail {
        
        private final byte[] picture;
        
        public Thumbnail(byte[] picture) {
            byte[] bytes = new byte[picture.length];
            System.arraycopy(picture, 0, bytes, 0, bytes.length);

            this.picture = bytes;
        }
        
        Thumbnail size(int length, int height) {
            return new Thumbnail(picture);
        }
        
        Thumbnail outputFormat(String format) {
            return new Thumbnail(picture);
        }
        
        CompletableFuture<byte[]> computeAsync() {
            return CompletableFuture.completedFuture(picture);
        }
        
        ListenableFuture<byte[]> compute() {
            return Futures.immediateFuture(picture);
        }
    }
    
}
